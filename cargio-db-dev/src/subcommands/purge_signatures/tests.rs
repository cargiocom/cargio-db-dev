use std::collections::BTreeSet;

use master_node::types::BlockHash;
use cargio_types::{ProtocolVersion, Signature, U512};
use lmdb::{Error as LmdbError, Transaction, WriteFlags};

use crate::{
    subcommands::purge_signatures::{
        block_signatures::BlockSignatures,
        purge::{initialize_indices, purge_signatures_for_blocks, EraWeights},
        Error,
    },
    test_utils::{self, LmdbTestFixture, MockBlockHeader, MockSwitchBlockHeader, KEYS},
};

fn get_sigs_from_db<T: Transaction>(
    txn: &T,
    fixture: &LmdbTestFixture,
    block_hash: &BlockHash,
) -> BlockSignatures {
    let serialized_sigs = txn
        .get(*fixture.db(Some("block_metadata")).unwrap(), block_hash)
        .unwrap();
    let block_sigs: BlockSignatures = bincode::deserialize(serialized_sigs).unwrap();
    assert_eq!(block_sigs.block_hash, *block_hash);
    block_sigs
}

#[test]
fn indices_initialization() {
    const BLOCK_COUNT: usize = 4;
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);

    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    block_headers[1].1.era_id = 10.into();
    block_headers[1].1.height = 200;
    block_headers[2].1.era_id = 20.into();
    block_headers[2].1.height = 300;
    block_headers[3].1.era_id = 20.into();
    block_headers[3].1.height = 400;
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 80;
    switch_block_headers[1].1.era_id = block_headers[2].1.era_id - 1;
    switch_block_headers[1].1.height = 280;

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in block_headers.iter().take(BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(&block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200, 300])).unwrap();
    assert_eq!(
        indices.heights.get(&block_headers[0].1.height).unwrap().0,
        block_headers[0].0
    );
    assert_eq!(
        indices.heights.get(&block_headers[1].1.height).unwrap().0,
        block_headers[1].0
    );
    assert_eq!(
        indices.heights.get(&block_headers[2].1.height).unwrap().0,
        block_headers[2].0
    );
    assert!(!indices.heights.contains_key(&block_headers[3].1.height));
    assert_eq!(
        *indices
            .switch_blocks
            .get(&block_headers[0].1.era_id)
            .unwrap(),
        switch_block_headers[0].0
    );
    assert_eq!(
        *indices
            .switch_blocks
            .get(&block_headers[2].1.era_id)
            .unwrap(),
        switch_block_headers[1].0
    );

    let (duplicate_hash, mut duplicate_header) = test_utils::mock_block_header(4);
    duplicate_header.height = block_headers[0].1.height;
    if let Ok(mut txn) = env.begin_rw_txn() {
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &duplicate_hash,
            &bincode::serialize(&duplicate_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    match initialize_indices(env, &BTreeSet::from([100, 200, 300])) {
        Err(Error::DuplicateBlock(height)) => assert_eq!(height, block_headers[0].1.height),
        _ => panic!("Unexpected error"),
    }
}

#[test]
fn indices_initialization_with_upgrade() {
    const BLOCK_COUNT: usize = 4;
    const SWITCH_BLOCK_COUNT: usize = 4;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 80;

    block_headers[1].1.era_id = 11.into();
    block_headers[1].1.height = 200;
    block_headers[2].1.protocol_version = ProtocolVersion::from_parts(1, 1, 0);

    block_headers[2].1.era_id = 12.into();
    block_headers[2].1.height = 290;
    block_headers[2].1.protocol_version = ProtocolVersion::from_parts(2, 0, 0);

    block_headers[3].1.era_id = 13.into();
    block_headers[3].1.height = 350;
    block_headers[3].1.protocol_version = ProtocolVersion::from_parts(2, 0, 0);

    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 60;

    switch_block_headers[1].1.era_id = block_headers[1].1.era_id - 1;
    switch_block_headers[1].1.height = 180;

    switch_block_headers[2].1.era_id = block_headers[2].1.era_id - 1;
    switch_block_headers[2].1.height = 250;
    switch_block_headers[2].1.protocol_version = ProtocolVersion::from_parts(1, 1, 0);

    switch_block_headers[3].1.height = 300;
    switch_block_headers[3].1.protocol_version = ProtocolVersion::from_parts(2, 0, 0);

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in block_headers.iter().take(BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(&block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200, 300])).unwrap();
    assert!(!indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[0].1.height));
    assert!(indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[1].1.height));
    assert!(indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[2].1.height));
    assert!(!indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[3].1.height));
}

#[test]
fn era_weights() {
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    switch_block_headers[0].1.era_id = 10.into();
    switch_block_headers[0].1.height = 80;
    switch_block_headers[0]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[0].clone(), 100.into());

    switch_block_headers[1].1.era_id = 20.into();
    switch_block_headers[1].1.height = 280;
    switch_block_headers[1]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[1].clone(), 100.into());

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };
    let indices = initialize_indices(env, &BTreeSet::from([80])).unwrap();
    let mut era_weights = EraWeights::default();
    if let Ok(txn) = env.begin_ro_txn() {
        let db = env.open_db(Some("block_header")).unwrap();
        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[0].1.era_id.successor()
            )
            .unwrap());
        assert_eq!(
            era_weights.era_id(),
            switch_block_headers[0].1.era_id.successor()
        );
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[0]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[1]));

        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());
        assert_eq!(
            era_weights.era_id(),
            switch_block_headers[1].1.era_id.successor()
        );
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());
        assert_eq!(
            era_weights.era_id(),
            switch_block_headers[1].1.era_id.successor()
        );
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

        let expected_missing_era_id = switch_block_headers[1].1.era_id.successor().successor();
        match era_weights.refresh_weights_for_era(&txn, db, &indices, expected_missing_era_id) {
            Err(Error::MissingEraWeights(actual_missing_era_id)) => {
                assert_eq!(expected_missing_era_id, actual_missing_era_id)
            }
            _ => panic!("Unexpected failure"),
        }
        txn.commit().unwrap();
    };

    if let Ok(mut txn) = env.begin_rw_txn() {
        switch_block_headers[0].1.era_end = None;
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &switch_block_headers[0].0,
            &bincode::serialize(&switch_block_headers[0].1).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };
    if let Ok(txn) = env.begin_ro_txn() {
        let db = env.open_db(Some("block_header")).unwrap();
        let expected_missing_era_id = switch_block_headers[0].1.era_id.successor();
        match era_weights.refresh_weights_for_era(&txn, db, &indices, expected_missing_era_id) {
            Err(Error::MissingEraWeights(actual_missing_era_id)) => {
                assert_eq!(expected_missing_era_id, actual_missing_era_id)
            }
            _ => panic!("Unexpected failure"),
        }
        txn.commit().unwrap();
    };
}

#[test]
fn era_weights_with_upgrade() {
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    switch_block_headers[0].1.era_id = 10.into();
    switch_block_headers[0].1.height = 80;
    switch_block_headers[0]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[0].clone(), 100.into());
    switch_block_headers[1].1.era_id = 11.into();
    switch_block_headers[1].1.height = 280;
    switch_block_headers[1]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[1].clone(), 100.into());
    switch_block_headers[1].1.protocol_version = ProtocolVersion::from_parts(1, 1, 0);

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };
    let indices = initialize_indices(env, &BTreeSet::from([80, 280])).unwrap();
    let mut era_weights = EraWeights::default();
    if let Ok(txn) = env.begin_ro_txn() {
        let db = env.open_db(Some("block_header")).unwrap();

        assert!(era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[0].1.era_id.successor()
            )
            .unwrap());

        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());

        assert!(era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[0].1.era_id.successor()
            )
            .unwrap());

        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());

        txn.commit().unwrap();
    };
}

#[test]
fn purge_signatures_should_work() {
    const BLOCK_COUNT: usize = 4;
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header", "block_metadata"], None);
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    block_headers[1].1.era_id = 10.into();
    block_headers[1].1.height = 200;
    block_headers[2].1.era_id = 20.into();
    block_headers[2].1.height = 300;
    block_headers[3].1.era_id = 20.into();
    block_headers[3].1.height = 400;
    let mut block_signatures: Vec<BlockSignatures> = block_headers
        .iter()
        .map(|(block_hash, header)| BlockSignatures::new(*block_hash, header.era_id))
        .collect();
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 80;
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[0].clone(), 500.into());
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[1].clone(), 500.into());

    block_signatures[0]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[0]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);
    block_signatures[1]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);

    switch_block_headers[1].1.era_id = block_headers[2].1.era_id - 1;
    switch_block_headers[1].1.height = 280;
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[0].clone(), 300.into());
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[1].clone(), 300.into());
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[2].clone(), 400.into());

    block_signatures[2]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[2]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);
    block_signatures[2]
        .proofs
        .insert(KEYS[2].clone(), Signature::System);
    block_signatures[3]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[3]
        .proofs
        .insert(KEYS[2].clone(), Signature::System);

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        for i in 0..BLOCK_COUNT {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            txn.put(
                *fixture.db(Some("block_metadata")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_signatures[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200, 300, 400])).unwrap();

    assert!(
        purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200, 300]), false).is_ok()
    );
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[0].0);
        assert!(
            (block_1_sigs.proofs.contains_key(&KEYS[0])
                && !block_1_sigs.proofs.contains_key(&KEYS[1]))
                || (!block_1_sigs.proofs.contains_key(&KEYS[0])
                    && block_1_sigs.proofs.contains_key(&KEYS[1]))
        );

        let block_2_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[1].0);
        assert!(block_2_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_2_sigs.proofs.contains_key(&KEYS[1]));

        let block_3_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[2].0);
        assert!(block_3_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_3_sigs.proofs.contains_key(&KEYS[1]));
        assert!(!block_3_sigs.proofs.contains_key(&KEYS[2]));

        let block_4_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[3].0);
        assert!(block_4_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_4_sigs.proofs.contains_key(&KEYS[1]));
        assert!(block_4_sigs.proofs.contains_key(&KEYS[2]));
        txn.commit().unwrap();
    };

    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 400]), true).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[0].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }

        let block_2_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[1].0);
        assert!(block_2_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_2_sigs.proofs.contains_key(&KEYS[1]));

        let block_3_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[2].0);
        assert!(block_3_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_3_sigs.proofs.contains_key(&KEYS[1]));
        assert!(!block_3_sigs.proofs.contains_key(&KEYS[2]));

        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[3].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }
        txn.commit().unwrap();
    };
}

#[test]
fn purge_signatures_bad_input() {
    const BLOCK_COUNT: usize = 2;
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header", "block_metadata"], None);
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    block_headers[1].1.era_id = 20.into();
    block_headers[1].1.height = 200;
    let mut block_signatures: Vec<BlockSignatures> = block_headers
        .iter()
        .map(|(block_hash, header)| BlockSignatures::new(*block_hash, header.era_id))
        .collect();
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 80;
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[0].clone(), 700.into());
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[1].clone(), 300.into());

    block_signatures[0]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[0]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);

    switch_block_headers[1].1.era_id = block_headers[1].1.era_id - 1;
    switch_block_headers[1].1.height = 180;
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[0].clone(), 400.into());
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[1].clone(), 600.into());

    block_signatures[1]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[1]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        for i in 0..BLOCK_COUNT {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            txn.put(
                *fixture.db(Some("block_metadata")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_signatures[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100])).unwrap();
    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), false).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[0].0);
        assert!(block_1_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_1_sigs.proofs.contains_key(&KEYS[1]));

        let block_2_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[1].0);
        assert!(block_2_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_2_sigs.proofs.contains_key(&KEYS[1]));
        txn.commit().unwrap();
    };

    if let Ok(mut txn) = env.begin_rw_txn() {
        txn.put(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[1].0,
            &bincode::serialize(&[0u8, 1u8, 2u8]).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200])).unwrap();
    match purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), false) {
        Err(Error::SignaturesParsing(block_hash, _)) if block_hash == block_headers[1].0 => {}
        other => panic!("Unexpected result: {other:?}"),
    };
}

#[test]
fn purge_signatures_missing_from_db() {
    const BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header", "block_metadata"], None);
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    block_headers[1].1.era_id = 10.into();
    block_headers[1].1.height = 200;
    let mut block_signatures: Vec<BlockSignatures> = block_headers
        .iter()
        .map(|(block_hash, header)| BlockSignatures::new(*block_hash, header.era_id))
        .collect();
    let (switch_block_hash, mut switch_block_header) = test_utils::mock_switch_block_header(0);
    switch_block_header.era_id = block_headers[0].1.era_id - 1;
    switch_block_header.height = 80;
    switch_block_header.insert_key_weight(KEYS[0].clone(), 400.into());
    switch_block_header.insert_key_weight(KEYS[1].clone(), 600.into());

    block_signatures[0]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[0]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in block_headers.iter().take(BLOCK_COUNT) {
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.put(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[0].0,
            &bincode::serialize(&block_signatures[0]).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &switch_block_hash,
            &bincode::serialize(&switch_block_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200])).unwrap();

    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), false).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[0].0);
        assert!(block_1_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_1_sigs.proofs.contains_key(&KEYS[1]));

        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[1].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }
        txn.commit().unwrap();
    };

    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), true).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[0].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }

        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[1].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }
        txn.commit().unwrap();
    };
}
