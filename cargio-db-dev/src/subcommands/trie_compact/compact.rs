use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    path::Path,
};

use log::info;

use cargio_hashing::Digest;

use crate::common::db::TRIE_STORE_FILE_NAME;

use super::{
    utils::{create_execution_engine, create_storage, load_execution_engine},
    Error,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DestinationOptions {
    Append,
    Overwrite,
    New,
}

fn validate_trie_paths<P1: AsRef<Path>, P2: AsRef<Path>>(
    source_trie_path: P1,
    destination_trie_path: P2,
    dest_opt: DestinationOptions,
) -> Result<(), Error> {
    let dest_path_exists = destination_trie_path.as_ref().exists();

    if !dest_path_exists {
        match dest_opt {
            DestinationOptions::New => {
                fs::create_dir_all(destination_trie_path.as_ref()).map_err(|err| {
                    Error::InvalidPath(destination_trie_path.as_ref().to_owned(), err)
                })?;
            }
            DestinationOptions::Append => {
                return Err(Error::InvalidDest(
                    "No destination trie to append. Consider not using \"--append\".".to_string(),
                ));
            }
            DestinationOptions::Overwrite => {
                return Err(Error::InvalidDest(
                    "No destination trie to overwrite. Consider not using \"--overwrite\"."
                        .to_string(),
                ));
            }
        }
    } else {
        let dest_data_exists = destination_trie_path
            .as_ref()
            .join(TRIE_STORE_FILE_NAME)
            .exists();
        match dest_opt {
            DestinationOptions::New => {
                if dest_data_exists {
                    return Err(Error::InvalidDest(format!(
                        "Output file \"data.lmdb\" already exists at destination \"{}\". \
                    Run the program with `--overwrite` to overwrite file or `--append` \
                    to write alongside existing contents.",
                        destination_trie_path
                            .as_ref()
                            .join(TRIE_STORE_FILE_NAME)
                            .to_string_lossy()
                    )));
                }
            }
            DestinationOptions::Append => {
                if !dest_data_exists {
                    return Err(Error::InvalidDest(format!(
                        "Nothing to append to, output file \"data.lmdb\" doesn't exist at \
                    destination \"{}\". Run the program without `--append`",
                        destination_trie_path
                            .as_ref()
                            .join(TRIE_STORE_FILE_NAME)
                            .to_string_lossy()
                    )));
                }
            }
            DestinationOptions::Overwrite => {
                if dest_data_exists {
                    let _f: File = OpenOptions::new()
                        .truncate(true)
                        .write(true)
                        .open(destination_trie_path.as_ref().join(TRIE_STORE_FILE_NAME))
                        .map_err(|io_err| {
                            Error::InvalidDest(format!(
                                "Couldn't overwrite destination file: {io_err}"
                            ))
                        })?;
                } else {
                    return Err(Error::InvalidDest(format!(
                        "Nothing to overwrite, output file \"data.lmdb\" doesn't exist at \
                        destination \"{}\". Run the program without `--overwrite`",
                        destination_trie_path
                            .as_ref()
                            .join(TRIE_STORE_FILE_NAME)
                            .to_string_lossy()
                    )));
                }
            }
        }
    }

    if source_trie_path
        .as_ref()
        .canonicalize()
        .map_err(|err| Error::InvalidPath(source_trie_path.as_ref().to_path_buf(), err))?
        == destination_trie_path
            .as_ref()
            .canonicalize()
            .map_err(|err| Error::InvalidPath(destination_trie_path.as_ref().to_path_buf(), err))?
    {
        return Err(Error::InvalidDest(
            "Destination trie can't be in the same LMDB file as the source.".to_string(),
        ));
    };

    Ok(())
}

pub fn trie_compact<P1: AsRef<Path>, P2: AsRef<Path>, P3: AsRef<Path>>(
    storage_path: P1,
    source_trie_path: P2,
    destination_trie_path: P3,
    dest_opt: DestinationOptions,
    max_db_size: usize,
) -> Result<(), Error> {
    validate_trie_paths(&source_trie_path, &destination_trie_path, dest_opt)?;

    let (source_state, _env) =
        load_execution_engine(source_trie_path, max_db_size, Digest::default(), true)
            .map_err(Error::OpenSourceTrie)?;

    let (destination_state, _env) =
        create_execution_engine(destination_trie_path, max_db_size, true)
            .map_err(Error::CreateDestTrie)?;

    let storage = create_storage(&storage_path).map_err(Error::OpenStorage)?;

    let mut block = match storage
        .read_highest_block()
        .map_err(|err| Error::Storage(0, err))?
    {
        Some(block) => block,
        None => {
            info!("No blocks found in storage, exiting.");
            return Ok(());
        }
    };
    let mut visited_roots = HashSet::new();
    let mut block_height;

    info!("Copying state roots from source to destination.");
    loop {
        block_height = block.height();
        let state_root = *block.take_header().state_root_hash();
        if !visited_roots.contains(&state_root) {
            super::helpers::copy_state_root(state_root, &source_state, &destination_state)
                .map_err(|err| Error::CopyStateRoot(state_root, err))?;
            destination_state
                .flush_environment()
                .map_err(Error::LmdbOperation)?;
            visited_roots.insert(state_root);
        }
        if block_height == 0 {
            break;
        }
        block = storage
            .read_block_by_height(block_height - 1)
            .map_err(|storage_err| Error::Storage(block_height - 1, storage_err))?
            .ok_or(Error::MissingBlock(block_height - 1))?;
    }
    info!(
        "Finished copying {} state roots to new database.",
        visited_roots.len()
    );

    Ok(())
}
