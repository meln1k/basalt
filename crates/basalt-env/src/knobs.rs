use crate::{Duration, Rng};

/// Declares the [`Knobs`] struct. Each knob has a type, a default, and the range simulation may
/// randomize it within.
macro_rules! knobs {
    ($(
        $(#[doc = $doc:literal])*
        $name:ident: $ty:ty = $default:expr, range $range:expr;
    )*) => {
        /// Every tuning constant: timeouts, batch sizes, retry intervals.
        ///
        /// Read them with [`knobs()`](crate::knobs). In production they are the defaults plus
        /// config; in simulation, some are randomized per seed within their declared range.
        #[derive(Clone, Debug, PartialEq)]
        pub struct Knobs {
            $(
                $(#[doc = $doc])*
                pub $name: $ty,
            )*
        }

        impl Default for Knobs {
            fn default() -> Knobs {
                Knobs { $($name: $default,)* }
            }
        }

        impl Knobs {
            /// Sets each knob for which `pick` returns `true` to a value drawn uniformly from its
            /// declared range, and returns their names. Knobs are visited in declaration order.
            pub fn randomize(
                &mut self,
                rng: &Rng,
                mut pick: impl FnMut(&'static str) -> bool,
            ) -> Vec<&'static str> {
                let mut picked = Vec::new();
                $(
                    if pick(stringify!($name)) {
                        self.$name = rng.range($range);
                        picked.push(stringify!($name));
                    }
                )*
                picked
            }

            #[cfg(test)]
            fn out_of_range(&self) -> Vec<&'static str> {
                let mut bad = Vec::new();
                $(
                    if !($range).contains(&self.$name) {
                        bad.push(stringify!($name));
                    }
                )*
                bad
            }
        }
    };
}

knobs! {
    /// The most extra delay BUGGIFY adds to a `Sleep` in simulation.
    max_buggified_delay: Duration = Duration::from_millis(200),
        range Duration::ZERO..=Duration::from_secs(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_within_their_ranges() {
        assert_eq!(Knobs::default().out_of_range(), Vec::<&str>::new());
    }

    #[test]
    fn randomize_stays_in_range_and_only_touches_picked_knobs() {
        let rng = Rng::new(7);
        let mut changed = false;
        for _ in 0..100 {
            let mut knobs = Knobs::default();
            assert_eq!(knobs.randomize(&rng, |_| true), ["max_buggified_delay"]);
            assert_eq!(knobs.out_of_range(), Vec::<&str>::new());
            changed |= knobs != Knobs::default();

            let mut untouched = Knobs::default();
            assert!(untouched.randomize(&rng, |_| false).is_empty());
            assert_eq!(untouched, Knobs::default());
        }
        assert!(changed);
    }

    #[test]
    fn randomize_is_seeded() {
        let draw = |seed| {
            let mut knobs = Knobs::default();
            knobs.randomize(&Rng::new(seed), |_| true);
            knobs
        };
        assert_eq!(draw(1), draw(1));
        assert_ne!(draw(1), draw(2));
    }
}
