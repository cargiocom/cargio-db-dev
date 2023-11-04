use std::fmt::{Display, Formatter, Result as FmtResult};

use cargio_hashing::Digest;
use master_node::types::DeployHash;
use cargio_types::PublicKey;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct BlockBody {
    proposer: PublicKey,
    pub deploy_hashes: Vec<DeployHash>,
    pub transfer_hashes: Vec<DeployHash>,
    #[serde(skip)]
    hash: OnceCell<Digest>,
}

impl BlockBody {
    #[cfg(test)]
    pub(crate) fn new(deploy_hashes: Vec<DeployHash>) -> Self {
        BlockBody {
            proposer: PublicKey::System,
            deploy_hashes,
            transfer_hashes: vec![],
            hash: OnceCell::new(),
        }
    }

    pub(crate) fn deploy_hashes(&self) -> &Vec<DeployHash> {
        &self.deploy_hashes
    }
}

impl Display for BlockBody {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        write!(
            formatter,
            "block body proposed by {}, {} deploys, {} transfers",
            self.proposer,
            self.deploy_hashes.len(),
            self.transfer_hashes.len()
        )?;
        Ok(())
    }
}
