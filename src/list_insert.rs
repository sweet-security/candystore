use bytemuck::{bytes_of, from_bytes};
use databuf::config::num::LE;
use databuf::Encode;
use uuid::Uuid;

use crate::encodable::EncodableUuid;
use crate::hashing::PartedHash;
use crate::shard::InsertMode;
use crate::{CandyStore, GetOrCreateStatus, ReplaceStatus, Result, SetStatus};

use crate::lists::{
    corrupted_list, update_chain_next, update_chain_prev, Chain, FullPartedHash, InsertPosition,
    InsertToListStatus, LinkedList,
};

impl CandyStore {
    pub(crate) fn _insert_to_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        mut val: Vec<u8>,
        mode: InsertMode,
        pos: InsertPosition,
    ) -> Result<InsertToListStatus> {
        let (list_ph, list_key) = self.make_list_key(list_key);
        let (item_ph, item_key) = self.make_item_key(list_ph, item_key);

        let _guard = self._list_lock(list_ph);

        // if the item already exists, it means it belongs to this list. we just need to update the value and
        // keep the existing chain part
        if let Some(mut old_val) = self.get_raw(&item_key)? {
            match mode {
                InsertMode::GetOrCreate => {
                    // don't replace the existing value
                    old_val.truncate(old_val.len() - size_of::<Chain>());
                    return Ok(InsertToListStatus::ExistingValue(old_val));
                }
                InsertMode::Replace(ev) => {
                    if ev.is_some_and(|ev| ev != &old_val[..old_val.len() - size_of::<Chain>()]) {
                        old_val.truncate(old_val.len() - size_of::<Chain>());
                        return Ok(InsertToListStatus::WrongValue(old_val));
                    }
                    // fall through
                }
                _ => {
                    // fall through
                }
            }

            val.extend_from_slice(&old_val[old_val.len() - size_of::<Chain>()..]);
            match self.replace_raw(&item_key, &val, None)? {
                ReplaceStatus::DoesNotExist => {
                    corrupted_list!("list {list_ph} failed replacing existing item {item_key:?}");
                }
                ReplaceStatus::PrevValue(mut v) => {
                    v.truncate(v.len() - size_of::<Chain>());
                    return Ok(InsertToListStatus::ExistingValue(v));
                }
                ReplaceStatus::WrongValue(_) => {
                    unreachable!();
                }
            }
        }

        if matches!(mode, InsertMode::Replace(_)) {
            // not allowed to create
            return Ok(InsertToListStatus::DoesNotExist);
        }

        let item_collidx = self._list_get_free_collidx(list_ph, item_ph)?;
        let item_fph = FullPartedHash::new(item_ph, item_collidx);

        // item does not exist, and the list itself might also not exist. get or create the list
        let curr_list = LinkedList::new(item_fph, item_fph);
        let curr_list = *from_bytes::<LinkedList>(
            &self
                .get_or_create_raw(&list_key, bytes_of(&curr_list).to_vec())?
                .value(),
        );

        // we have the list. if the list points to this item, it means we've just created it
        if curr_list.full_head() == item_fph {
            if curr_list.full_tail() != item_fph {
                corrupted_list!(
                    "list {list_ph} head ({}) != tail ({})",
                    curr_list.full_head(),
                    curr_list.full_tail(),
                );
            }
            // this first item needs to have prev=INVALID and next=INVALID
            val.extend_from_slice(bytes_of(&Chain::new(
                item_collidx,
                FullPartedHash::INVALID,
                FullPartedHash::INVALID,
            )));
            if !self.set_raw(&item_key, &val)?.was_created() {
                corrupted_list!("list {list_ph} expected to create {item_fph} {item_key:?}");
            }
            val.truncate(val.len() - size_of::<Chain>());
            return Ok(InsertToListStatus::CreatedNew(val));
        }

        //self.fixup_true_ends(list_ph, curr_list)?;

        let v =
            match pos {
                InsertPosition::Tail => self
                    ._insert_to_list_tail(list_ph, list_key, item_fph, item_key, val, curr_list)?,
                InsertPosition::Head => self
                    ._insert_to_list_head(list_ph, list_key, item_fph, item_key, val, curr_list)?,
            };

        Ok(InsertToListStatus::CreatedNew(v))
    }

    fn _insert_to_list_head(
        &self,
        list_ph: PartedHash,
        list_key: Vec<u8>,
        item_fph: FullPartedHash,
        item_key: Vec<u8>,
        mut val: Vec<u8>,
        curr_list: LinkedList,
    ) -> Result<Vec<u8>> {
        // the list already exists. start at list.head and find the true head (it's possible list.
        // isn't up to date because of crashes)
        let (head_fph, head_k, mut head_v) = self.find_true_head(list_ph, curr_list.full_head())?;

        // modify the current head item to point to the new item. if we crash after this, everything is okay because
        // find_true_head will stop at this item
        update_chain_prev(&mut head_v, item_fph);
        if self.replace_raw(&head_k, &head_v, None)?.failed() {
            corrupted_list!("list {list_ph:?} failed to point {head_k:?}->prev to {item_key:?}");
        }

        // now add item, with prev pointing to the old head. if we crash after this, find_head_tail
        // will return the newly-added item as the head.
        // possible optimization: only update the head every X operations, this reduces the expected
        // number of IOs at the expense of more walking when inserting
        let this_chain = Chain::new(item_fph.collidx, head_fph, FullPartedHash::INVALID);
        val.extend_from_slice(bytes_of(&this_chain));
        if !self.set_raw(&item_key, &val)?.was_created() {
            corrupted_list!("list {list_ph:?} tail {item_key:?} already exists");
        }

        // now update the list to point to the new tail. if we crash before it's committed, all's good
        let new_list = LinkedList::new(item_fph, curr_list.full_tail());
        if self
            .replace_raw(&list_key, bytes_of(&new_list), Some(bytes_of(&curr_list)))?
            .failed()
        {
            corrupted_list!("list {item_fph} failed to point head to {item_key:?}");
        }

        val.truncate(val.len() - size_of::<Chain>());
        Ok(val)
    }

    fn _insert_to_list_tail(
        &self,
        list_ph: PartedHash,
        list_key: Vec<u8>,
        item_fph: FullPartedHash,
        item_key: Vec<u8>,
        mut val: Vec<u8>,
        curr_list: LinkedList,
    ) -> Result<Vec<u8>> {
        // the list already exists. start at list.tail and find the true tail (it's possible list.tail
        // isn't up to date because of crashes)
        let (tail_fph, tail_k, mut tail_v) = self.find_true_tail(list_ph, curr_list.full_tail())?;

        // modify the last item to point to the new item. if we crash after this, everything is okay because
        // find_true_tail will stop at this item
        update_chain_next(&mut tail_v, item_fph);

        if self.replace_raw(&tail_k, &tail_v, None)?.failed() {
            corrupted_list!("list {list_ph:?} failed to point {tail_k:?}->next to {item_key:?}");
        }

        // now add item, with prev pointing to the old tail. if we crash after this, find_true_tail
        // will return the newly-added item as the tail.
        // possible optimization: only update the tail every X operations, this reduces the expected
        // number of IOs at the expense of more walking when inserting
        let this_chain = Chain::new(item_fph.collidx, FullPartedHash::INVALID, tail_fph);
        val.extend_from_slice(bytes_of(&this_chain));
        if self.set_raw(&item_key, &val)?.was_replaced() {
            corrupted_list!("list {list_ph:?} tail {item_key:?} already exists");
        }

        // now update the list to point to the new tail. if we crash before it's committed, all's good
        let mut new_list = curr_list.clone();
        new_list.set_full_tail(item_fph);
        if self
            .replace_raw(&list_key, bytes_of(&new_list), Some(bytes_of(&curr_list)))?
            .failed()
        {
            corrupted_list!("list {list_ph:?} failed to point tail to {item_key:?}");
        }

        val.truncate(val.len() - size_of::<Chain>());
        Ok(val)
    }

    /// Sets (or replaces) an item (identified by `item_key`) in a linked-list (identified by `list_key`) -
    /// placing the item at the tail (end) of the list. Linked lists are created when the first item is
    /// inserted to them, and removed when the last item is removed.
    ///
    /// If the item already exists in the list, its value is replaced but it keeps is relative position.
    ///
    /// See also [Self::set]
    pub fn set_in_list<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<SetStatus> {
        self.owned_set_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            val.as_ref().to_owned(),
            false,
        )
    }

    /// Sets (or replaces) an item (identified by `item_key`) in a linked-list (identified by `list_key`) -
    /// placing the item at the tail (end) of the list. If the item already exists in the list,
    /// it is re-inserted at the end.
    ///
    /// This allows for the implementation of LRUs, where older items stay at the beginning and more
    /// recent ones are at the end.
    ///
    /// Note: this operation is not crash-safe, as it removes and inserts the item.
    ///
    /// See also [Self::set], [Self::set_in_list]
    pub fn set_in_list_promoting<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<SetStatus> {
        self.owned_set_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            val.as_ref().to_owned(),
            true,
        )
    }

    // Owned version of set_in_list, takes `promote` as a parameter instead
    pub fn owned_set_in_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        val: Vec<u8>,
        promote: bool,
    ) -> Result<SetStatus> {
        if promote {
            self.owned_remove_from_list(list_key.clone(), item_key.clone())?;
        }
        match self._insert_to_list(
            list_key,
            item_key,
            val,
            InsertMode::Set,
            InsertPosition::Tail,
        )? {
            InsertToListStatus::CreatedNew(_) => Ok(SetStatus::CreatedNew),
            InsertToListStatus::ExistingValue(v) => Ok(SetStatus::PrevValue(v)),
            InsertToListStatus::DoesNotExist => unreachable!(),
            InsertToListStatus::WrongValue(_) => unreachable!(),
        }
    }

    /// Same as [Self::set_in_list], but will only replace an existing item (will not create one if the key
    /// does not already exist). See also [Self::replace]
    pub fn replace_in_list<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
        expected_val: Option<&B3>,
    ) -> Result<ReplaceStatus> {
        self.owned_replace_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            val.as_ref().to_owned(),
            expected_val.map(|ev| ev.as_ref()),
        )
    }

    /// Owned version of [Self::replace_in_list]
    pub fn owned_replace_in_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        val: Vec<u8>,
        expected_val: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        match self._insert_to_list(
            list_key,
            item_key,
            val,
            InsertMode::Replace(expected_val),
            InsertPosition::Tail,
        )? {
            InsertToListStatus::DoesNotExist => Ok(ReplaceStatus::DoesNotExist),
            InsertToListStatus::ExistingValue(v) => Ok(ReplaceStatus::PrevValue(v)),
            InsertToListStatus::WrongValue(v) => Ok(ReplaceStatus::WrongValue(v)),
            InsertToListStatus::CreatedNew(_) => unreachable!(),
        }
    }

    /// Returns the existing value of the element in the list, if it exists, or create it with the given
    /// default value.
    ///
    /// See also [Self::get_or_create]
    pub fn get_or_create_in_list<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<GetOrCreateStatus> {
        self.owned_get_or_create_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            val.as_ref().to_owned(),
        )
    }

    /// Owned version of [Self::get_or_create_in_list]
    pub fn owned_get_or_create_in_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        val: Vec<u8>,
    ) -> Result<GetOrCreateStatus> {
        match self._insert_to_list(
            list_key,
            item_key,
            val,
            InsertMode::GetOrCreate,
            InsertPosition::Tail,
        )? {
            InsertToListStatus::CreatedNew(v) => Ok(GetOrCreateStatus::CreatedNew(v)),
            InsertToListStatus::ExistingValue(v) => Ok(GetOrCreateStatus::ExistingValue(v)),
            InsertToListStatus::DoesNotExist => unreachable!(),
            InsertToListStatus::WrongValue(_) => unreachable!(),
        }
    }

    /// In case you only want to store values in a list (the keys are immaterial), this function
    /// generates a random UUID and inserts the given element to the end (tail) of the list.
    /// Can be used to implement queues, where elements are pushed at the back and popped from
    /// the front.
    ///
    /// The function returns the generated UUID, and you can use it to access the item
    /// using functions like [Self::remove_from_list], etc., but it's not the canonical use case
    pub fn push_to_list_tail<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        val: &B2,
    ) -> Result<EncodableUuid> {
        self.owned_push_to_list_tail(list_key.as_ref().to_owned(), val.as_ref().to_owned())
    }

    /// Owned version of [Self::push_to_list]
    pub fn owned_push_to_list_tail(
        &self,
        list_key: Vec<u8>,
        val: Vec<u8>,
    ) -> Result<EncodableUuid> {
        let uuid = EncodableUuid::from(Uuid::new_v4());
        let status = self._insert_to_list(
            list_key,
            uuid.to_bytes::<LE>(),
            val,
            InsertMode::GetOrCreate,
            InsertPosition::Tail,
        )?;
        if !matches!(status, InsertToListStatus::CreatedNew(_)) {
            corrupted_list!("list uuid collision {uuid} {status:?}");
        }
        Ok(uuid)
    }

    /// In case you only want to store values in a list (the keys are immaterial), this function
    /// generates a random UUID and inserts the given element to the head (head) of the list.
    /// Can be used to implement queues, where elements are pushed at the back and popped from
    /// the front.
    ///
    /// The function returns the generated UUID, and you can use it to access the item
    /// using functions like [Self::remove_from_list], etc., but it's not the canonical use case
    pub fn push_to_list_head<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        val: &B2,
    ) -> Result<EncodableUuid> {
        self.owned_push_to_list_head(list_key.as_ref().to_owned(), val.as_ref().to_owned())
    }

    /// Owned version of [Self::push_to_list_head]
    pub fn owned_push_to_list_head(
        &self,
        list_key: Vec<u8>,
        val: Vec<u8>,
    ) -> Result<EncodableUuid> {
        let uuid = EncodableUuid::from(Uuid::new_v4());
        let status = self._insert_to_list(
            list_key,
            uuid.to_bytes::<LE>(),
            val,
            InsertMode::GetOrCreate,
            InsertPosition::Head,
        )?;
        if !matches!(status, InsertToListStatus::CreatedNew(_)) {
            corrupted_list!("uuid collision {uuid} {status:?}");
        }
        Ok(uuid)
    }
}
