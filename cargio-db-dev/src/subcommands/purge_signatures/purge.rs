use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    path::Path,
};

use cargio_hashing::Digest;
use master_node::types::{BlockHash, BlockHeader};
use cargio_types::{EraId, ProtocolVersion, PublicKey, U512};
use lmdb::{Cursor, Database, Environment, Error as LmdbError, Transaction, WriteFlags};
use log::{error, info, warn};

use crate::common::{
    db::{self, BlockHeaderDatabase, BlockMetadataDatabase, Database as _, STORAGE_FILE_NAME},
    lmdb_utils,
    progress::ProgressTracker,
};

use super::{block_signatures::BlockSignatures, signatures::strip_signatures, Error};

#[derive(Default)]
pub(crate) struct Indices {
    pub(crate) heights: BTreeMap<u64, (BlockHash, BlockHeader)>,
    pub(crate) switch_blocks: BTreeMap<EraId, BlockHash>,
    pub(crate) switch_blocks_before_upgrade: BTreeSet<u64>,
}

#[derive(Default)]
pub(crate) struct EraWeights {
    era_id: EraId,
    weights: BTreeMap<PublicKey, U512>,
    era_after_upgrade: bool,
}

impl EraWeights {
    pub(crate) fn refresh_weights_for_era<T: Transaction>(
        &mut self,
        txn: &T,
        db: Database,
        indices: &Indices,
        era_id: EraId,
    ) -> Result<bool, Error> {
        if self.era_id == era_id {
            return Ok(self.era_after_upgrade);
        }
        let switch_block_hash = indices
            .switch_blocks
            .get(&era_id)
            .ok_or_else(|| Error::MissingEraWeights(era_id))?;
        let switch_block_header: BlockHeader =
            bincode::deserialize(txn.get(db, &switch_block_hash)?)
                .map_err(|bincode_err| Error::HeaderParsing(*switch_block_hash, bincode_err))?;
        self.era_after_upgrade = indices
            .switch_blocks_before_upgrade
            .contains(&switch_block_header.height());
        let weights = switch_block_header
            .next_era_validator_weights()
            .cloned()
            .ok_or_else(|| Error::MissingEraWeights(era_id))?;
        self.weights = weights;
        self.era_id = era_id;
        Ok(self.era_after_upgrade)
    }

    #[cfg(test)]
    pub(crate) fn era_id(&self) -> EraId {
        self.era_id
    }

    #[cfg(test)]
    pub(crate) fn weights_mut(&mut self) -> &mut BTreeMap<PublicKey, U512> {
        &mut self.weights
    }
}

pub(crate) fn initialize_indices(
    env: &Environment,
    needed_heights: &BTreeSet<u64>,
) -> Result<Indices, Error> {
    let mut indices = Indices::default();
    let txn = env.begin_ro_txn()?;
    let header_db = unsafe { txn.open_db(Some(BlockHeaderDatabase::db_name()))? };

    let mut maybe_progress_tracker = match lmdb_utils::entry_count(&txn, header_db).ok() {
        Some(entry_count) => Some(
            ProgressTracker::new(
                entry_count,
                Box::new(|completion| info!("Header database parsing {}% complete...", completion)),
            )
            .map_err(|_| Error::EmptyDatabase)?,
        ),
        None => {
            info!("Skipping progress tracking for header database parsing");
            None
        }
    };

    {
        let mut last_blocks_before_upgrade: BTreeMap<ProtocolVersion, u64> = BTreeMap::default();
        let mut cursor = txn.open_ro_cursor(header_db)?;
        for (raw_key, raw_value) in cursor.iter() {
            if let Some(progress_tracker) = maybe_progress_tracker.as_mut() {
                progress_tracker.advance_by(1);
            }
            let block_hash: BlockHash = match Digest::try_from(raw_key) {
                Ok(digest) => digest.into(),
                Err(digest_parsing_err) => {
                    error!("Skipping block header because of invalid hash {raw_key:?}: {digest_parsing_err}");
                    continue;
                }
            };
            let block_header: BlockHeader = bincode::deserialize(raw_value)
                .map_err(|bincode_err| Error::HeaderParsing(block_hash, bincode_err))?;
            let block_height = block_header.height();
            if block_header.is_switch_block() {
                let _ = indices
                    .switch_blocks
                    .insert(block_header.era_id().successor(), block_hash);
                match last_blocks_before_upgrade.entry(block_header.protocol_version()) {
                    Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(block_height);
                    }
                    Entry::Occupied(mut occupied_entry) => {
                        if *occupied_entry.get() < block_height {
                            occupied_entry.insert(block_height);
                        }
                    }
                }
            }
            if needed_heights.contains(&block_height)
                && indices
                    .heights
                    .insert(block_height, (block_hash, block_header))
                    .is_some()
            {
                return Err(Error::DuplicateBlock(block_height));
            };
        }
        let _ = last_blocks_before_upgrade.pop_last();
        indices
            .switch_blocks_before_upgrade
            .extend(last_blocks_before_upgrade.into_values());
    }
    txn.commit()?;
    Ok(indices)
}

pub(crate) fn purge_signatures_for_blocks(
    env: &Environment,
    indices: &Indices,
    heights_to_visit: BTreeSet<u64>,
    full_purge: bool,
) -> Result<(), Error> {
    let mut txn = env.begin_rw_txn()?;
    let header_db = unsafe { txn.open_db(Some(BlockHeaderDatabase::db_name()))? };
    let signatures_db = unsafe { txn.open_db(Some(BlockMetadataDatabase::db_name()))? };

    let mut era_weights = EraWeights::default();

    let mut progress_tracker = ProgressTracker::new(
        heights_to_visit.len(),
        Box::new(if full_purge {
            |completion| {
                info!(
                    "Signature purging to no finality {}% complete...",
                    completion
                )
            }
        } else {
            |completion| {
                info!(
                    "Signature purging to weak finality {}% complete...",
                    completion
                )
            }
        }),
    )
    .map_err(|_| Error::EmptyBlockList)?;

    for height in heights_to_visit {
        let (block_hash, block_header) = match indices.heights.get(&height) {
            Some((block_hash, block_header)) => {
                if block_header.era_id().is_genesis() {
                    warn!("Cannot strip signatures for genesis block");
                    progress_tracker.advance_by(1);
                    continue;
                }
                (block_hash, block_header)
            }
            None => {
                warn!("Block at height {height} is not present in the database");
                progress_tracker.advance_by(1);
                continue;
            }
        };
        let block_height = block_header.height();
        let era_id = block_header.era_id();
        let era_after_upgrade =
            era_weights.refresh_weights_for_era(&txn, header_db, indices, era_id)?;

        let mut block_signatures: BlockSignatures = match txn.get(signatures_db, &block_hash) {
            Ok(raw_signatures) => bincode::deserialize(raw_signatures)
                .map_err(|bincode_err| Error::SignaturesParsing(*block_hash, bincode_err))?,
            Err(LmdbError::NotFound) => {
                warn!(
                    "No signature entry in the database for block \
                    {block_hash} at height {block_height}"
                );
                progress_tracker.advance_by(1);
                continue;
            }
            Err(lmdb_err) => return Err(Error::Database(lmdb_err)),
        };

        if full_purge {
            txn.del(signatures_db, &block_hash, None)?;
        } else if strip_signatures(&mut block_signatures, &era_weights.weights) {
            if era_after_upgrade {
                warn!(
                    "Using possibly inaccurate weights to purge signatures \
                    for block {block_hash} at height {block_height}"
                );
            }
            let serialized_signatures = bincode::serialize(&block_signatures)
                .map_err(|bincode_err| Error::Serialize(*block_hash, bincode_err))?;
            txn.put(
                signatures_db,
                &block_hash,
                &serialized_signatures,
                WriteFlags::default(),
            )?;
        } else {
            warn!("Couldn't strip signatures for block {block_hash} at height {block_height}");
        }
        progress_tracker.advance_by(1);
    }
    txn.commit()?;
    Ok(())
}

pub fn purge_signatures<P: AsRef<Path>>(
    db_path: P,
    weak_finality_block_list: BTreeSet<u64>,
    no_finality_block_list: BTreeSet<u64>,
) -> Result<(), Error> {
    let storage_path = db_path.as_ref().join(STORAGE_FILE_NAME);
    let env = db::db_env(storage_path)?;
    let heights_to_visit = weak_finality_block_list
        .union(&no_finality_block_list)
        .copied()
        .collect();
    let indices = initialize_indices(&env, &heights_to_visit)?;
    if !weak_finality_block_list.is_empty() {
        purge_signatures_for_blocks(&env, &indices, weak_finality_block_list, false)?;
    }
    if !no_finality_block_list.is_empty() {
        purge_signatures_for_blocks(&env, &indices, no_finality_block_list, true)?;
    }
    Ok(())
}
