use std::result::Result;

use lmdb::{Error as LmdbError, RoTransaction, RwTransaction, Transaction, WriteFlags};

pub(crate) fn read_from_db<K: AsRef<[u8]>>(
    txn: &mut RoTransaction,
    db_name: &str,
    key: &K,
) -> Result<Vec<u8>, LmdbError> {
    let db = unsafe { txn.open_db(Some(db_name))? };
    let value = txn.get(db, key)?.to_vec();
    Ok(value)
}

pub(crate) fn write_to_db<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    txn: &mut RwTransaction,
    db_name: &str,
    key: &K,
    value: &V,
) -> Result<(), LmdbError> {
    let db = unsafe { txn.open_db(Some(db_name))? };
    txn.put(db, key, value, WriteFlags::empty())?;
    Ok(())
}

pub(crate) fn transfer_to_new_db<K: AsRef<[u8]>>(
    source_txn: &mut RoTransaction,
    destination_txn: &mut RwTransaction,
    db_name: &str,
    key: &K,
) -> Result<Vec<u8>, LmdbError> {
    let value = read_from_db(source_txn, db_name, key)?;
    write_to_db(destination_txn, db_name, key, &value)?;
    Ok(value)
}
