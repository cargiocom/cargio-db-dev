use std::{path::Path, result::Result};

use cargio_hashing::Digest;
use log::info;

use crate::subcommands::trie_compact::{
    copy_state_root, create_execution_engine, load_execution_engine, DEFAULT_MAX_DB_SIZE,
};

use super::Error;

pub(crate) fn transfer_global_state<P1: AsRef<Path>, P2: AsRef<Path>>(
    source: P1,
    destination: P2,
    state_root_hash: Digest,
) -> Result<(), Error> {
    let max_db_size = DEFAULT_MAX_DB_SIZE
        .parse()
        .expect("should be able to parse max db size");

    let (source_state, _env) = load_execution_engine(source, max_db_size, Digest::default(), true)
        .map_err(Error::LoadExecutionEngine)?;
    let (destination_state, _env) = create_execution_engine(destination, max_db_size, true)
        .map_err(Error::CreateExecutionEngine)?;
    info!("Starting transfer process for state root hash {state_root_hash}");
    copy_state_root(state_root_hash, &source_state, &destination_state)
        .map_err(Error::StateRootTransfer)?;
    destination_state.flush_environment()?;

    Ok(())
}
