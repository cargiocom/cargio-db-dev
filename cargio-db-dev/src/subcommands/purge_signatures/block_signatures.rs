use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
};

use master_node::types::BlockHash;
use cargio_types::{EraId, PublicKey, Signature};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialOrd, Ord, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct BlockSignatures {
    pub(crate) block_hash: BlockHash,
    pub(crate) era_id: EraId,
    pub(crate) proofs: BTreeMap<PublicKey, Signature>,
}

#[cfg(test)]
impl BlockSignatures {
    pub(crate) fn new(block_hash: BlockHash, era_id: EraId) -> Self {
        Self {
            block_hash,
            era_id,
            proofs: Default::default(),
        }
    }
}

impl Display for BlockSignatures {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(
            formatter,
            "block signatures for hash: {} in era_id: {} with {} proofs",
            self.block_hash,
            self.era_id,
            self.proofs.len()
        )
    }
}
