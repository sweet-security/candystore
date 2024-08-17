use bytemuck::{bytes_of, from_bytes};

use crate::{
    hashing::PartedHash,
    lists::{
        chain_of, corrupted_list, update_chain_next, update_chain_prev, Chain, FullPartedHash,
        LinkedList, ITEM_SUFFIX_LEN,
    },
    shard::KVPair,
    CandyStore, Result,
};

impl CandyStore {
    fn _remove_from_list_head(
        &self,
        mut list: LinkedList,
        chain: Chain,
        list_ph: PartedHash,
        list_key: &[u8],
        item_key: &[u8],
    ) -> Result<()> {
        let Some((next_k, mut next_v)) = self._list_get(list_ph, chain.full_next())? else {
            corrupted_list!("list {list_ph} failed getting next of {item_key:?}");
        };

        // update list.head from this to this.next. if we crash afterwards, the list will start
        // at the expected place.
        list.set_full_head(chain.full_next());
        if self.replace_raw(list_key, bytes_of(&list), None)?.failed() {
            corrupted_list!(
                "list {list_ph} failed pointing list head to point to {}",
                chain.full_next()
            );
        }

        // set the new head's prev link to INVALID. if we crash afterwards, everything is good.
        update_chain_prev(&mut next_v, FullPartedHash::INVALID);
        if self.replace_raw(&next_k, &next_v, None)?.failed() {
            corrupted_list!(
                "list {list_ph} failed updating prev=INVALID on the now-first {next_k:?} element"
            );
        }

        // finally remove the item, sealing the deal
        self.remove_raw(item_key)?;
        Ok(())
    }

    fn _remove_from_list_tail(
        &self,
        mut list: LinkedList,
        chain: Chain,
        list_ph: PartedHash,
        list_key: &[u8],
        item_key: &[u8],
    ) -> Result<()> {
        let Some((prev_k, mut prev_v)) = self._list_get(list_ph, chain.full_prev())? else {
            corrupted_list!("list {list_ph} missing prev element {item_key:?}");
        };

        // point list.tail to the prev item. if we crash afterwards, the removed tail is still considered
        // part of the list (find_true_tail will find it)
        list.set_full_tail(chain.full_prev());
        if !self
            .replace_raw(list_key, bytes_of(&list), None)?
            .was_replaced()
        {
            corrupted_list!(
                "failed updating list {list_ph} tail to point to prev {}",
                chain.full_prev()
            );
        }

        // XXX clear the item's chain so we can scrub it later?

        // update the new tail's next to INVALID. if we crash afterwards, the removed tail is no longer
        // considered part of the list
        update_chain_next(&mut prev_v, FullPartedHash::INVALID);
        if self.replace_raw(&prev_k, &prev_v, None)?.failed() {
            corrupted_list!(
                "list {list_ph} failed updating next=INVALID on the now-last {prev_k:?} element"
            );
        }

        // finally remove the item, sealing the deal
        self.remove_raw(item_key)?;
        Ok(())
    }

    fn _remove_from_list_middle(
        &self,
        chain: Chain,
        list_ph: PartedHash,
        item_fph: FullPartedHash,
        item_key: &[u8],
    ) -> Result<()> {
        // this is a "middle" item, it has a prev one and a next one. set prev.next = this.next,
        // set next.prev = prev, update list (for `len`)
        // it might now have prev or next, in case we crashed after disconnecting one of them, so just
        // continue from where we left off

        if chain.full_prev().is_valid() {
            if let Some((prev_k, mut prev_v)) = self._list_get(list_ph, chain.full_prev())? {
                if chain_of(&prev_v).full_next() == item_fph {
                    update_chain_next(&mut prev_v, chain.full_next());
                    if self.replace_raw(&prev_k, &prev_v, None)?.failed() {
                        corrupted_list!("list {list_ph} failed updating prev.next on {prev_k:?}");
                    }
                }
            }
        }

        if chain.full_next().is_valid() {
            if let Some((next_k, mut next_v)) = self._list_get(list_ph, chain.full_next())? {
                if chain_of(&next_v).full_prev() == item_fph {
                    update_chain_prev(&mut next_v, chain.full_prev());
                    if self.replace_raw(&next_k, &next_v, None)?.failed() {
                        corrupted_list!("list {list_ph} failed updating next.prev on {next_k:?}");
                    }
                }
            }
        }

        // now it's safe to remove the item
        self.remove_raw(item_key)?;
        Ok(())
    }

    pub fn remove_from_list<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        self.owned_remove_from_list(list_key.as_ref().to_owned(), item_key.as_ref().to_owned())
    }

    fn _remove_from_list(
        &self,
        list: LinkedList,
        list_key: &[u8],
        list_ph: PartedHash,
        item_key: &[u8],
        item_fph: FullPartedHash,
        chain: Chain,
    ) -> Result<()> {
        // because of the crash model, it's possible list.head and list.tail are not up to date.
        // it's also possible that we'll leak some entries if we crash mid-operation, i.e., an item
        // might have been unlinked from its prev or next, but still exists on its own.
        // XXX: maybe background compaction can check for leaks and remove them?

        let mut head_fph = list.full_head();
        if item_fph == head_fph && chain.full_prev().is_valid() {
            let (true_head_fph, _, _) = self.find_true_head(list_ph, head_fph)?;
            head_fph = true_head_fph;
        }

        let mut tail_fph = list.full_tail();
        if item_fph == tail_fph && chain.full_next().is_valid() {
            let (true_tail_fph, _, _) = self.find_true_tail(list_ph, tail_fph)?;
            tail_fph = true_tail_fph;
        }

        if item_fph == head_fph && item_fph == tail_fph {
            // it's the only element in the list
            self.remove_raw(list_key)?;
            self.remove_raw(item_key)?;
        } else if item_fph == head_fph {
            // it's the head
            self._remove_from_list_head(list, chain, list_ph, list_key, item_key)?
        } else if item_fph == tail_fph {
            // it's the tail
            self._remove_from_list_tail(list, chain, list_ph, list_key, item_key)?
        } else {
            // it's a middle element
            self._remove_from_list_middle(chain, list_ph, item_fph, item_key)?
        }

        Ok(())
    }

    /// Owned version of [Self::remove_from_list]
    pub fn owned_remove_from_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        let (list_ph, list_key) = self.make_list_key(list_key);
        let (item_ph, item_key) = self.make_item_key(list_ph, item_key);

        let _guard = self._list_lock(list_ph);

        // if the item does not exist -- all's good
        let Some(mut v) = self.get_raw(&item_key)? else {
            return Ok(None);
        };

        let chain = chain_of(&v);
        v.truncate(v.len() - size_of::<Chain>());
        let item_fph = FullPartedHash::new(item_ph, chain._this_collidx);

        // fetch the list
        let Some(list_buf) = self.get_raw(&list_key)? else {
            // if it does not exist, it means we've crashed right between removing the list and removing
            // the only item it held - proceed to removing this item
            self.remove_raw(&item_key)?;
            return Ok(Some(v));
        };

        let list = *from_bytes::<LinkedList>(&list_buf);
        self._remove_from_list(list, &list_key, list_ph, &item_key, item_fph, chain)?;

        Ok(Some(v))
    }

    /// Removes and returns the first (head) element from the list
    pub fn pop_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_pop_list_head(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::pop_list_head]
    pub fn owned_pop_list_head(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        let (list_ph, list_key) = self.make_list_key(list_key);

        let _guard = self._list_lock(list_ph);

        let Some(list_buf) = self.get_raw(&list_key)? else {
            return Ok(None);
        };
        let list = *from_bytes::<LinkedList>(&list_buf);

        let (item_fph, mut item_key, mut item_val) =
            self.find_true_head(list_ph, list.full_head())?;
        let chain = chain_of(&item_val);
        item_val.truncate(item_val.len() - size_of::<Chain>());

        self._remove_from_list(list, &list_key, list_ph, &item_key, item_fph, chain)?;

        item_key.truncate(item_key.len() - ITEM_SUFFIX_LEN);
        Ok(Some((item_key, item_val)))
    }

    /// Removes and returns the last (tail) element from the list
    pub fn pop_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_pop_list_tail(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::pop_list_tail]
    pub fn owned_pop_list_tail(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        let (list_ph, list_key) = self.make_list_key(list_key);

        let _guard = self._list_lock(list_ph);

        let Some(list_buf) = self.get_raw(&list_key)? else {
            return Ok(None);
        };
        let list = *from_bytes::<LinkedList>(&list_buf);

        let (item_fph, mut item_key, mut item_val) =
            self.find_true_tail(list_ph, list.full_tail())?;
        let chain = chain_of(&item_val);
        item_val.truncate(item_val.len() - size_of::<Chain>());

        self._remove_from_list(list, &list_key, list_ph, &item_key, item_fph, chain)?;

        item_key.truncate(item_key.len() - ITEM_SUFFIX_LEN);
        Ok(Some((item_key, item_val)))
    }
}
