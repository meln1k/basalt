use std::cell::Cell;
use std::fmt;
use std::ops::{Range, RangeInclusive};
use std::time::Duration;

/// A deterministic pseudo-random number generator: xoshiro256** by Blackman and Vigna.
///
/// We implement it ourselves instead of using the `rand` crate, so a given seed produces the same
/// numbers forever, whatever our dependencies do. Every method's output for a given state is part
/// of that contract, including how [`range`](Rng::range) maps raw output to a range: changing any
/// of them changes what every seed does.
///
/// Methods take `&self`, because [`Ctx::rng`](crate::Ctx::rng) hands out a shared reference to the
/// world's generator.
///
/// # Streams
///
/// A world doesn't draw everything from one generator: with one stream, a single extra draw
/// anywhere shifts every later draw, so flipping a flag or adding a retry reshuffles the faults
/// too. Instead the simulator gives each concern its own stream with [`derive`](Rng::derive),
/// named rather than numbered, so adding a stream never shifts the others. [`fork`](Rng::fork)
/// splits off an owned generator, for example to move into a [`Ctx::compute`](crate::Ctx::compute)
/// job, which can't reach `ctx.rng()`.
pub struct Rng {
    state: Cell<[u64; 4]>,
}

impl Rng {
    /// A generator whose state is four outputs of SplitMix64 seeded with `seed`, as the
    /// xoshiro authors recommend.
    pub fn new(seed: u64) -> Rng {
        let mut x = seed;
        let mut next = || splitmix64(&mut x);
        Rng::from_state([next(), next(), next(), next()])
    }

    /// The stream named `stream` of the run seeded with `seed`. Different names give independent
    /// generators, and each depends only on `seed` and its own name.
    pub fn derive(seed: u64, stream: &str) -> Rng {
        let mut x = seed;
        Rng::new(splitmix64(&mut x) ^ fnv1a(stream.as_bytes()))
    }

    /// A new generator seeded from this one's next output. Forking advances this generator by
    /// exactly one draw, and the child is as deterministic as its parent.
    pub fn fork(&self) -> Rng {
        Rng::new(self.u64())
    }

    /// A generator with exactly this state. Panics if the state is all zeros, the one state
    /// xoshiro256** can't leave.
    pub fn from_state(state: [u64; 4]) -> Rng {
        assert!(state != [0; 4], "xoshiro256** state must not be all zeros");
        Rng {
            state: Cell::new(state),
        }
    }

    /// The current state, for hashing into the unseed.
    pub fn state(&self) -> [u64; 4] {
        self.state.get()
    }

    /// The next 64 random bits.
    pub fn u64(&self) -> u64 {
        let [mut s0, mut s1, mut s2, mut s3] = self.state.get();
        let result = s1.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s1 << 17;
        s2 ^= s0;
        s3 ^= s1;
        s1 ^= s2;
        s0 ^= s3;
        s2 ^= t;
        s3 = s3.rotate_left(45);
        self.state.set([s0, s1, s2, s3]);
        result
    }

    /// A uniform float in `[0, 1)`, from the top 53 bits of [`u64`](Rng::u64).
    pub fn f64(&self) -> f64 {
        (self.u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// `true` with probability `p`. Panics unless `0 <= p <= 1`.
    pub fn bool(&self, p: f64) -> bool {
        assert!(
            (0.0..=1.0).contains(&p),
            "probability {p} is outside [0, 1]"
        );
        self.f64() < p
    }

    /// A uniform value in `range`, which may be `a..b` or `a..=b` over the integer types and
    /// [`Duration`]. Panics if the range is empty.
    pub fn range<R: SampleRange>(&self, range: R) -> R::Item {
        range.sample(self)
    }

    /// Shuffles `items` uniformly (Fisher-Yates).
    pub fn shuffle<T>(&self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }

    /// A uniform value in `0..n`, without modulo bias (Lemire's method). `n` must not be zero.
    fn below(&self, n: u64) -> u64 {
        debug_assert!(n > 0);
        let mut m = u128::from(self.u64()) * u128::from(n);
        if (m as u64) < n {
            let threshold = n.wrapping_neg() % n;
            while (m as u64) < threshold {
                m = u128::from(self.u64()) * u128::from(n);
            }
        }
        (m >> 64) as u64
    }
}

impl fmt::Debug for Rng {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rng").finish_non_exhaustive()
    }
}

fn splitmix64(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// FNV-1a, 64-bit: a fixed, published hash, so stream names map to the same seeds forever.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// A range [`Rng::range`] can draw from.
pub trait SampleRange {
    type Item;
    fn sample(self, rng: &Rng) -> Self::Item;
}

macro_rules! int_ranges {
    ($($t:ty as $wide:ty),*) => {$(
        impl SampleRange for Range<$t> {
            type Item = $t;
            fn sample(self, rng: &Rng) -> $t {
                assert!(self.start < self.end, "empty range {:?}", self);
                let span = (self.end as $wide).wrapping_sub(self.start as $wide) as u64;
                (self.start as $wide).wrapping_add(rng.below(span) as $wide) as $t
            }
        }

        impl SampleRange for RangeInclusive<$t> {
            type Item = $t;
            fn sample(self, rng: &Rng) -> $t {
                let (lo, hi) = self.into_inner();
                assert!(lo <= hi, "empty range {lo:?}..={hi:?}");
                let span = (hi as $wide).wrapping_sub(lo as $wide) as u64;
                let offset = match span.checked_add(1) {
                    Some(n) => rng.below(n),
                    None => rng.u64(),
                };
                (lo as $wide).wrapping_add(offset as $wide) as $t
            }
        }
    )*};
}

int_ranges!(
    u8 as u64,
    u16 as u64,
    u32 as u64,
    u64 as u64,
    usize as u64,
    i32 as i64,
    i64 as i64
);

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).expect("duration too long to sample")
}

impl SampleRange for Range<Duration> {
    type Item = Duration;
    fn sample(self, rng: &Rng) -> Duration {
        Duration::from_nanos(rng.range(nanos(self.start)..nanos(self.end)))
    }
}

impl SampleRange for RangeInclusive<Duration> {
    type Item = Duration;
    fn sample(self, rng: &Rng) -> Duration {
        Duration::from_nanos(rng.range(nanos(*self.start())..=nanos(*self.end())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashSet;

    // Expected values come from Vigna's reference C code (prng.di.unimi.it/xoshiro256starstar.c
    // and splitmix64.c), compiled and run separately; they are not derived from this module.

    #[test]
    fn xoshiro256starstar_matches_reference() {
        let rng = Rng::from_state([1, 2, 3, 4]);
        let expected: [u64; 10] = [
            11520,
            0,
            1509978240,
            1215971899390074240,
            1216172134540287360,
            607988272756665600,
            16172922978634559625,
            8476171486693032832,
            10595114339597558777,
            2904607092377533576,
        ];
        for want in expected {
            assert_eq!(rng.u64(), want);
        }
    }

    #[test]
    fn splitmix64_matches_reference() {
        let mut x = 0;
        let expected: [u64; 5] = [
            16294208416658607535,
            7960286522194355700,
            487617019471545679,
            17909611376780542444,
            1961750202426094747,
        ];
        for want in expected {
            assert_eq!(splitmix64(&mut x), want);
        }
    }

    #[test]
    fn seeding_matches_reference() {
        let rng = Rng::new(4711);
        let expected: [u64; 5] = [
            17952372374560461915,
            5649350752990243679,
            6255228628137916910,
            12503752556551753465,
            12483383906795520154,
        ];
        for want in expected {
            assert_eq!(rng.u64(), want);
        }
    }

    #[test]
    fn fnv1a_matches_reference() {
        assert_eq!(fnv1a(b""), 14695981039346656037);
        assert_eq!(fnv1a(b"a"), 12638187200555641996);
        assert_eq!(fnv1a(b"foobar"), 9625390261332436968);
    }

    #[test]
    fn derive_matches_reference() {
        let faults = Rng::derive(4711, "faults");
        for want in [
            5764250993647121660,
            7643640308960252852,
            10925654694563423509,
        ] {
            assert_eq!(faults.u64(), want);
        }
        let network = Rng::derive(4711, "network");
        for want in [
            7543580247626127578,
            3907027297585027036,
            18440091866216699476,
        ] {
            assert_eq!(network.u64(), want);
        }
    }

    #[test]
    fn derived_streams_are_independent_and_unaffected_by_other_streams() {
        let draws = |rng: &Rng| (0..1000).map(|_| rng.u64()).collect::<Vec<_>>();
        let faults = draws(&Rng::derive(1, "faults"));

        // Drawing from other streams, or adding new ones, leaves this one unchanged.
        let network = draws(&Rng::derive(1, "network"));
        let _ = draws(&Rng::derive(1, "a-stream-added-later"));
        assert_eq!(draws(&Rng::derive(1, "faults")), faults);

        // Neither a copy nor a shifted copy of another stream or another seed's stream.
        let seen: HashSet<u64> = faults.iter().copied().collect();
        for other in [
            network,
            draws(&Rng::derive(2, "faults")),
            draws(&Rng::new(1)),
        ] {
            assert!(other.iter().all(|x| !seen.contains(x)));
        }
    }

    #[test]
    fn fork_matches_reference_and_advances_the_parent_by_one_draw() {
        let parent = Rng::new(4711);
        let child = parent.fork();
        for want in [
            12991305653467911195,
            5290987989673639116,
            7611990594197924436,
        ] {
            assert_eq!(child.u64(), want);
        }
        // The second output of `Rng::new(4711)`, as in `seeding_matches_reference`.
        assert_eq!(parent.u64(), 5649350752990243679);
    }

    #[test]
    fn a_fork_can_move_to_another_thread() {
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&Rng::new(0).fork());
    }

    #[test]
    #[should_panic(expected = "all zeros")]
    fn zero_state_is_rejected() {
        Rng::from_state([0; 4]);
    }

    #[test]
    fn ranges_cover_both_ends_and_nothing_outside() {
        let rng = Rng::new(1);
        let mut seen = [false; 7];
        for _ in 0..2_000 {
            let x = rng.range(-3i64..=3);
            seen[(x + 3) as usize] = true;
            assert!((10..13u32).contains(&rng.range(10u32..13)));
        }
        assert_eq!(
            seen, [true; 7],
            "-3..=3 must hit every value, including both ends"
        );

        for _ in 0..100 {
            assert_eq!(rng.range(5u64..6), 5);
            assert_eq!(rng.range(7u8..=7), 7);
            assert_eq!(rng.range(u8::MAX..=u8::MAX), u8::MAX);
        }
        // Full-width ranges must not overflow.
        rng.range(0..=u64::MAX);
        rng.range(i64::MIN..=i64::MAX);
        rng.range(i64::MIN..i64::MAX);
    }

    #[test]
    fn range_is_unbiased_for_a_span_that_does_not_divide_2_pow_64() {
        // Modulo reduction over 3 * 2^62 would pick the first 2^62 values twice as often.
        let rng = Rng::new(2);
        let span = 3u64 << 62;
        let n = 30_000;
        let low = (0..n).filter(|_| rng.range(0..span) < (1 << 62)).count();
        let share = low as f64 / n as f64;
        assert!(
            (share - 1.0 / 3.0).abs() < 0.02,
            "share of lowest third: {share}"
        );
    }

    #[test]
    fn duration_ranges() {
        let rng = Rng::new(3);
        let lo = Duration::from_millis(1);
        let hi = Duration::from_millis(2);
        for _ in 0..100 {
            let d = rng.range(lo..hi);
            assert!(lo <= d && d < hi);
        }
        assert_eq!(rng.range(hi..=hi), hi);
    }

    #[test]
    #[should_panic(expected = "empty range")]
    fn empty_range_panics() {
        Rng::new(0).range(5u32..5);
    }

    #[test]
    fn bool_extremes_and_rate() {
        let rng = Rng::new(4);
        assert!((0..1000).all(|_| !rng.bool(0.0)));
        assert!((0..1000).all(|_| rng.bool(1.0)));
        let hits = (0..20_000).filter(|_| rng.bool(0.25)).count();
        assert!((4_500..5_500).contains(&hits), "hits: {hits}");
    }

    #[test]
    fn f64_is_in_unit_interval() {
        let rng = Rng::from_state([u64::MAX; 4]);
        for _ in 0..1000 {
            let x = rng.f64();
            assert!((0.0..1.0).contains(&x));
        }
    }

    #[test]
    fn shuffle_is_a_seeded_permutation() {
        let shuffled = |seed| {
            let mut v: Vec<u32> = (0..50).collect();
            Rng::new(seed).shuffle(&mut v);
            v
        };
        let a = shuffled(9);
        let mut sorted = a.clone();
        sorted.sort();
        assert_eq!(sorted, (0..50).collect::<Vec<_>>());
        assert_eq!(a, shuffled(9));
        assert_ne!(a, shuffled(10));
        assert_ne!(a, (0..50).collect::<Vec<_>>());
    }
}
