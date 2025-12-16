use crate::types::{
    CandyError, EntryPointer, HashCoordinates, KeyNamespace, Result, SpecialEntryType,
};
use simd_itertools::PositionSimd;

use super::CandyStore;

impl CandyStore {
    pub(crate) fn _get_special_key(
        &self,
        ns: KeyNamespace,
        special_type: SpecialEntryType,
        key: u64,
    ) -> Result<Option<u64>> {
        self.inner.get_special_key(ns, special_type, key)
    }

    pub(crate) fn _upsert_special_key(
        &self,
        ns: KeyNamespace,
        special_type: SpecialEntryType,
        key: u64,
        mut f: impl FnMut(Option<u64>) -> u64,
    ) -> Result<Option<u64>> {
        let key_bytes = key.to_le_bytes();
        let hc = HashCoordinates::from_key(
            self.inner.config.hash_key.0,
            self.inner.config.hash_key.1,
            ns,
            &key_bytes,
        );

        self.inner
            .index_file
            .operate_on_row_mut(hc, |row, _header| {
                for (idx, ptr) in row.iter_matches(hc) {
                    if let Some((type_id, old_val)) = ptr.get_special_value()
                        && type_id == special_type as u8
                    {
                        let new_val = f(Some(old_val));
                        row.pointers[idx] =
                            EntryPointer::new_special(special_type, new_val, hc.row_selector);
                        return Ok(Some(old_val));
                    }
                }

                if let Some(idx) = row
                    .signatures
                    .iter()
                    .position_simd(|&sig| sig == HashCoordinates::INVALID_SIG)
                {
                    let new_val = f(None);
                    row.pointers[idx] =
                        EntryPointer::new_special(special_type, new_val, hc.row_selector);
                    row.signatures[idx] = hc.signature;
                    Ok(None)
                } else {
                    Err(CandyError::SplitRow)
                }
            })
    }

    pub(crate) fn update_file_stats(&self, file_id: u16, wasted: u64, _removed: u64) -> Result<()> {
        let key = file_id as u64;

        if wasted > 0 {
            self._upsert_special_key(
                KeyNamespace::StatsWastedBytes,
                SpecialEntryType::WastedBytes,
                key,
                |old| old.unwrap_or(0) + wasted,
            )?;
        }

        Ok(())
    }
}
