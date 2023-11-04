use std::collections::{BTreeMap, BTreeSet};

use cargio_types::{PublicKey, U512};

use super::block_signatures::BlockSignatures;

fn is_weak_finality(weight: U512, total: U512) -> bool {
    weight * 3 > total
}

fn is_strict_finality(weight: U512, total: U512) -> bool {
    weight * 3 > total * 2
}

pub(super) fn strip_signatures(
    signatures: &mut BlockSignatures,
    weights: &BTreeMap<PublicKey, U512>,
) -> bool {
    let total_weight: U512 = weights
        .iter()
        .map(|(_, weight)| weight)
        .fold(U512::zero(), |acc, weight| acc + *weight);

    let mut inverse_map: BTreeMap<U512, Vec<&PublicKey>> = BTreeMap::default();
    for (key, weight) in weights.iter() {
        inverse_map.entry(*weight).or_default().push(key);
    }
    let mut accumulated_sigs: BTreeSet<&PublicKey> = Default::default();
    let mut accumulated_weight = U512::zero();
    for (weight, key) in inverse_map
        .iter()
        .flat_map(|(weight, keys)| keys.iter().map(move |key| (weight, *key)))
    {
        if signatures.proofs.contains_key(key) {
            accumulated_weight += *weight;
            accumulated_sigs.insert(key);

            if is_weak_finality(accumulated_weight, total_weight) {
                break;
            }
        }
    }
    while is_strict_finality(accumulated_weight, total_weight) {
        if accumulated_sigs.is_empty() {
            return false;
        }
        let popped_sig = accumulated_sigs.pop_first().unwrap();
        let popped_sig_weight = weights.get(popped_sig).unwrap();
        accumulated_weight -= *popped_sig_weight;
    }
    if !is_weak_finality(accumulated_weight, total_weight) {
        return false;
    }
    signatures
        .proofs
        .retain(|key, _| accumulated_sigs.contains(key));
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cargio_types::{PublicKey, Signature, U512};

    use crate::{
        subcommands::purge_signatures::{
            block_signatures::BlockSignatures,
            signatures::{is_strict_finality, is_weak_finality, strip_signatures},
        },
        test_utils::KEYS,
    };

    #[test]
    fn weak_finality() {
        assert!(!is_weak_finality(1.into(), 3.into()));
        assert!(!is_weak_finality(0.into(), 1_000.into()));
        assert!(!is_weak_finality(10.into(), 1_000.into()));
        assert!(!is_weak_finality(333_333.into(), 1_000_000.into()));

        assert!(is_weak_finality(333_334.into(), 1_000_000.into()));
        assert!(is_weak_finality(666_667.into(), 1_000_000.into()));
        assert!(is_weak_finality(1_000_000.into(), 1_000_000.into()));
    }

    #[test]
    fn strict_finality() {
        assert!(!is_strict_finality(2.into(), 3.into()));
        assert!(!is_strict_finality(0.into(), 1000.into()));
        assert!(!is_strict_finality(10.into(), 1000.into()));
        assert!(!is_strict_finality(333_333.into(), 1_000_000.into()));
        assert!(!is_strict_finality(333_334.into(), 1_000_000.into()));
        assert!(!is_strict_finality(666_666.into(), 1_000_000.into()));

        assert!(is_strict_finality(666_667.into(), 1_000_000.into()));
        assert!(is_strict_finality(900.into(), 1000.into()));
        assert!(is_strict_finality(1000.into(), 1000.into()));
    }

    #[test]
    fn strip_signatures_progressive() {
        let mut block_signatures = BlockSignatures::default();
        block_signatures
            .proofs
            .insert(KEYS[0].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[1].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[2].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[3].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        weights.insert(KEYS[0].clone(), 100.into());
        weights.insert(KEYS[1].clone(), 200.into());
        weights.insert(KEYS[2].clone(), 300.into());
        weights.insert(KEYS[3].clone(), 400.into());

        assert!(strip_signatures(&mut block_signatures, &weights));
        assert!(block_signatures.proofs.contains_key(&KEYS[0]));
        assert!(block_signatures.proofs.contains_key(&KEYS[1]));
        assert!(block_signatures.proofs.contains_key(&KEYS[2]));
        assert!(!block_signatures.proofs.contains_key(&KEYS[3]));
    }

    #[test]
    fn strip_signatures_equal_weights() {
        let mut block_signatures = BlockSignatures::default();
        block_signatures
            .proofs
            .insert(KEYS[0].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[1].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        weights.insert(KEYS[0].clone(), 500.into());
        weights.insert(KEYS[1].clone(), 500.into());

        assert!(strip_signatures(&mut block_signatures, &weights));
        assert_eq!(block_signatures.proofs.len(), 1);
    }

    #[test]
    fn strip_signatures_one_small_three_large() {
        let mut block_signatures = BlockSignatures::default();
        block_signatures
            .proofs
            .insert(KEYS[0].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[1].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[2].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[3].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        weights.insert(KEYS[0].clone(), 1.into());
        weights.insert(KEYS[1].clone(), 333.into());
        weights.insert(KEYS[2].clone(), 333.into());
        weights.insert(KEYS[3].clone(), 333.into());

        assert!(strip_signatures(&mut block_signatures, &weights));
        assert!(block_signatures.proofs.contains_key(&KEYS[0]));
        assert_eq!(block_signatures.proofs.len(), 2);
    }

    #[test]
    fn strip_signatures_split_weights() {
        let mut block_signatures = BlockSignatures::default();
        block_signatures
            .proofs
            .insert(KEYS[0].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[1].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[2].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        weights.insert(KEYS[0].clone(), 333.into());
        weights.insert(KEYS[1].clone(), 333.into());
        weights.insert(KEYS[2].clone(), 333.into());

        assert!(strip_signatures(&mut block_signatures, &weights));
        assert_eq!(block_signatures.proofs.len(), 2);
    }

    #[test]
    fn strip_signatures_one_key_has_strict_finality() {
        let mut block_signatures = BlockSignatures::default();
        block_signatures
            .proofs
            .insert(KEYS[0].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[1].clone(), Signature::System);
        block_signatures
            .proofs
            .insert(KEYS[2].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        weights.insert(KEYS[0].clone(), 100.into());
        weights.insert(KEYS[1].clone(), 200.into());
        weights.insert(KEYS[2].clone(), 700.into());
        assert!(!strip_signatures(&mut block_signatures, &weights));
    }

    #[test]
    fn strip_signatures_single_key() {
        let mut block_signatures = BlockSignatures::default();
        block_signatures
            .proofs
            .insert(KEYS[0].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        weights.insert(KEYS[0].clone(), 1000.into());
        assert!(!strip_signatures(&mut block_signatures, &weights));
    }
}
