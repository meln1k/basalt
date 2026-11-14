//! Hash maps and sets whose iteration order depends only on what was inserted.
//!
//! `std`'s default hasher draws random keys per map, so iteration order changes between runs of
//! the same seed. These aliases use a fixed hasher instead. Create them with `default()` or
//! `with_capacity_and_hasher(n, FixedState::default())`; `new()` exists only for the random
//! hasher.

use std::hash::{BuildHasherDefault, Hasher};

// The aliases are the one sanctioned use of the std types, which core code may not name.
#[allow(clippy::disallowed_types)]
/// A [`std::collections::HashMap`] with a fixed hasher.
pub type HashMap<K, V> = std::collections::HashMap<K, V, FixedState>;

#[allow(clippy::disallowed_types)]
/// A [`std::collections::HashSet`] with a fixed hasher.
pub type HashSet<T> = std::collections::HashSet<T, FixedState>;

/// Builds [`FxHasher`]s, which have no random state.
pub type FixedState = BuildHasherDefault<FxHasher>;

/// The Fx hash from rustc and Firefox: fast and fixed, but not resistant to chosen keys, which
/// is fine for keys we generate ourselves. Like rustc-hash 2, it rotates the high bits down at
/// the end, because the multiply leaves the low bits of aligned keys (offsets, page numbers)
/// zero, and the map picks buckets from the low bits.
#[derive(Clone, Copy, Debug, Default)]
pub struct FxHasher {
    hash: u64,
}

const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(SEED);
    }
}

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for word in words {
            self.add(u64::from_le_bytes(*word));
        }
        if !rest.is_empty() {
            let mut word = [0; 8];
            word[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(word));
        }
    }

    fn write_u8(&mut self, i: u8) {
        self.add(u64::from(i));
    }

    fn write_u16(&mut self, i: u16) {
        self.add(u64::from(i));
    }

    fn write_u32(&mut self, i: u32) {
        self.add(u64::from(i));
    }

    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }

    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }

    fn finish(&self) -> u64 {
        self.hash.rotate_left(26)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iteration_order_is_the_same_for_the_same_inserts() {
        // With std's random hasher, two maps built the same way iterate in different orders.
        let build = || {
            let mut map = HashMap::default();
            for i in 0..1000u32 {
                map.insert(format!("key-{i}"), i);
            }
            map.into_iter().collect::<Vec<_>>()
        };
        assert_eq!(build(), build());

        let set = |n: u64| {
            (0..n)
                .collect::<HashSet<u64>>()
                .into_iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(set(500), set(500));
    }

    #[test]
    fn aligned_keys_spread_over_the_low_bits() {
        // Keys that are multiples of 2^20 multiply to hashes whose low 20 bits are zero, so
        // without a final mix every one of them lands in the same bucket of a small table.
        use std::hash::BuildHasher;
        let low_bits: HashSet<u64> = (0..4096u64)
            .map(|i| FixedState::default().hash_one(i << 20) & 0xfff)
            .collect();
        assert!(
            low_bits.len() > 2000,
            "only {} distinct low 12-bit values",
            low_bits.len()
        );
    }
}
