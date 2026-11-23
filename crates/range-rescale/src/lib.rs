/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Exact rescaling of a `u128` key range onto the full `u64` range.
//!
//! [`Rescaler`] maps keys in `[lo, hi]` linearly onto `0..=u64::MAX`, the
//! coordinate space a space-filling curve indexes. It runs once per row per
//! clustering column, so it replaces the `u128` division the formula implies
//! (a software routine on every target) with a multiplication by a reciprocal
//! computed once per range, plus one remainder correction.
//!
//! That replacement is only correct if three things hold together: the
//! reciprocal estimate is never more than one below the true quotient, the
//! correction never underflows, and no intermediate product overflows `u128`.
//! None of these is visible by inspection, and a test can only sample them. So
//! they are postconditions the [Verus](https://github.com/verus-lang/verus)
//! verifier discharges for every input:
//!
//! - [`Rescaler::rescale`] returns exactly the value [`Rescaler::expected`]
//!   specifies, and never overflows or panics.
//! - [`Rescaler::lemma_rescale_monotonic`]: a larger key never rescales to a
//!   smaller value, which is the property clustering depends on.
//!
//! This is the executable code, not a model of it: the functions below are the
//! ones callers run. Verus reads the `verus!` block; a normal `cargo build`
//! erases the specifications and proofs and compiles the bodies as ordinary
//! Rust. `cargo verus focus` re-checks the proofs.

// The lemmas are proof code, which a normal build erases along with every call
// to them, so they are only imported when Verus keeps ghost code.
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::div_mod::{
    lemma_div_is_ordered, lemma_div_pos_is_pos, lemma_fundamental_div_mod,
    lemma_fundamental_div_mod_converse, lemma_mod_bound,
};
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::mul::{
    lemma_mul_is_distributive_add, lemma_mul_is_distributive_add_other_way,
};
use vstd::prelude::*;

verus! {

/// `2^64`: one limb of a `u128`.
const LIMB: u128 = 0x1_0000_0000_0000_0000;

/// The largest rescaled value, `u64::MAX`, as a `u128`.
const MAX_CODE: u128 = 0xffff_ffff_ffff_ffff;

/// `2^128`, the modulus of `u128` arithmetic.
spec fn p128() -> int {
    LIMB as int * LIMB as int
}

/// Rescales keys from `[lo, hi]` onto `0..=u64::MAX`.
///
/// With `x = clamp(key, lo, hi) - lo`, `span = hi - lo`, and
/// `shift = max(0, bit_width(span) - 64)`, [`Rescaler::rescale`] computes
/// `floor((x >> shift) * u64::MAX / (span >> shift))` exactly.
///
/// - Keys outside `[lo, hi]` clamp to its ends.
/// - A constant or inverted range (`hi <= lo`) carries no information to
///   spread, so every key maps to 0.
/// - `x * u64::MAX` must fit in `u128`, which needs `span < 2^64`, so `shift`
///   is 0 for those spans. For a wider span the low `shift` bits of both `x`
///   and `span` are dropped first. The result can then differ slightly from
///   `floor(x * u64::MAX / span)`, but it stays monotonic in `key`.
pub struct Rescaler {
    lo: u128,
    hi: u128,
    shift: u128,
    span: u128,
    reciprocal: u128,
}

impl Rescaler {
    /// What every constructed `Rescaler` satisfies: `span` is the shifted
    /// range width and fits in a `u64`, it is zero exactly when the range is
    /// constant or inverted, and `reciprocal` is `floor((2^128 - 1) / span)`
    /// whenever the division is not trivial.
    #[verifier::type_invariant]
    spec fn inv(self) -> bool {
        &&& self.shift <= 64
        &&& self.span <= MAX_CODE
        &&& self.span == (if self.hi > self.lo { (self.hi - self.lo) as u128 } else { 0u128 }) >> self.shift
        &&& (self.span == 0 <==> self.hi <= self.lo)
        &&& (self.span >= 2 ==> self.reciprocal == u128::MAX / self.span)
    }

    /// The value [`Rescaler::rescale`] returns for `key`.
    pub closed spec fn expected(&self, key: u128) -> int {
        if self.hi <= self.lo || self.span == 0 {
            0
        } else {
            let clamped = if key < self.lo { self.lo } else if key > self.hi { self.hi } else { key };
            let x = ((clamped - self.lo) as u128) >> self.shift;
            (x as int) * (MAX_CODE as int) / (self.span as int)
        }
    }

    /// Prepares to rescale keys from `[lo, hi]`.
    #[must_use]
    pub fn new(lo: u128, hi: u128) -> Self {
        let full: u128 = hi.saturating_sub(lo);
        let mut shift: u128 = 0;
        let mut span: u128 = full;
        proof {
            lemma_shr_zero(full);
        }
        while span > MAX_CODE
            invariant
                shift <= 64,
                span == full >> shift,
                full >= 1 ==> span >= 1,
            decreases 64 - shift,
        {
            proof {
                lemma_shr_wide_means_short_shift(full, shift);
                lemma_shr_step(full, shift);
                lemma_half_of_wide_is_positive(span);
            }
            span >>= 1u128;
            shift += 1;
        }
        let reciprocal = if span > 1 { u128::MAX / span } else { 0 };
        proof {
            lemma_zero_shr(shift);
        }
        Rescaler { lo, hi, shift, span, reciprocal }
    }

    /// Rescales `key`; see [`Rescaler`] for the exact formula.
    #[inline]
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the quotient is proved to be at most u64::MAX, so the cast is exact"
    )]
    pub fn rescale(&self, key: u128) -> (r: u64)
        ensures
            r as int == self.expected(key),
    {
        proof {
            use_type_invariant(self);
        }
        if self.span == 0 {
            return 0;
        }
        let clamped = if key < self.lo {
            self.lo
        } else if key > self.hi {
            self.hi
        } else {
            key
        };
        let x = (clamped - self.lo) >> self.shift;
        proof {
            lemma_shr_monotone((clamped - self.lo) as u128, (self.hi - self.lo) as u128, self.shift);
            assert(x as int * MAX_CODE as int <= MAX_CODE as int * MAX_CODE as int) by (nonlinear_arith)
                requires
                    x <= MAX_CODE,
            ;
        }
        let numerator = x * MAX_CODE;
        let quotient = if self.span == 1 {
            proof {
                lemma_fundamental_div_mod_converse(numerator as int, 1, numerator as int, 0);
            }
            numerator
        } else {
            let estimate = multiply_high(numerator, self.reciprocal);
            proof {
                lemma_reciprocal_estimate(
                    numerator as int,
                    self.span as int,
                    self.reciprocal as int,
                    estimate as int,
                    p128(),
                );
            }
            let remainder = numerator - estimate * self.span;
            if remainder >= self.span {
                proof {
                    let (n, d, e) = (numerator as int, self.span as int, estimate as int);
                    assert((e + 1) * d == e * d + d) by (nonlinear_arith);
                    lemma_fundamental_div_mod_converse(n, d, e + 1, n - (e + 1) * d);
                }
                estimate + 1
            } else {
                proof {
                    let (n, d, e) = (numerator as int, self.span as int, estimate as int);
                    lemma_fundamental_div_mod_converse(n, d, e, n - e * d);
                }
                estimate
            }
        };
        proof {
            lemma_quotient_fits(x as int, self.span as int);
        }
        quotient as u64
    }

    /// A larger key never rescales to a smaller value.
    pub proof fn lemma_rescale_monotonic(&self, a: u128, b: u128)
        requires
            a <= b,
        ensures
            self.expected(a) <= self.expected(b),
    {
        if !(self.hi <= self.lo || self.span == 0) {
            let ca = if a < self.lo { self.lo } else if a > self.hi { self.hi } else { a };
            let cb = if b < self.lo { self.lo } else if b > self.hi { self.hi } else { b };
            let xa = ((ca - self.lo) as u128) >> self.shift;
            let xb = ((cb - self.lo) as u128) >> self.shift;
            lemma_shr_monotone((ca - self.lo) as u128, (cb - self.lo) as u128, self.shift);
            assert(xa as int * MAX_CODE as int <= xb as int * MAX_CODE as int) by (nonlinear_arith)
                requires
                    xa <= xb,
            ;
            lemma_div_is_ordered(
                xa as int * MAX_CODE as int,
                xb as int * MAX_CODE as int,
                self.span as int,
            );
        }
    }
}

/// The upper half of the 256-bit product `a * b`, from four 64-bit limb
/// products.
#[inline]
fn multiply_high(a: u128, b: u128) -> (r: u128)
    ensures
        r as int == (a as int * b as int) / p128(),
{
    let a0 = a % LIMB;
    let a1 = a / LIMB;
    let b0 = b % LIMB;
    let b1 = b / LIMB;
    proof {
        lemma_limb_product_fits(a0 as int, b0 as int);
        lemma_limb_product_fits(a1 as int, b0 as int);
        lemma_limb_product_fits(a0 as int, b1 as int);
        lemma_limb_product_fits(a1 as int, b1 as int);
    }
    let low = a0 * b0;
    let left = a1 * b0;
    let right = a0 * b1;
    let middle = low / LIMB + left % LIMB + right % LIMB;
    proof {
        lemma_multiply_high(a as int, b as int);
    }
    a1 * b1 + left / LIMB + right / LIMB + middle / LIMB
}

/// A product of two limbs fits in a `u128`.
proof fn lemma_limb_product_fits(x: int, y: int)
    requires
        0 <= x < LIMB as int,
        0 <= y < LIMB as int,
    ensures
        0 <= x * y < p128(),
{
    assert(0 <= x * y <= (LIMB as int - 1) * (LIMB as int - 1)) by (nonlinear_arith)
        requires
            0 <= x <= LIMB as int - 1,
            0 <= y <= LIMB as int - 1,
    ;
}

/// Schoolbook multiplication over 64-bit limbs: the high limb sum computed by
/// [`multiply_high`] is the floor of the full product over `2^128`.
proof fn lemma_multiply_high(a: int, b: int)
    requires
        0 <= a < p128(),
        0 <= b < p128(),
    ensures
        ({
            let l = LIMB as int;
            let (a0, a1, b0, b1) = (a % l, a / l, b % l, b / l);
            let (low, left, right) = (a0 * b0, a1 * b0, a0 * b1);
            let middle = low / l + left % l + right % l;
            &&& a1 * b1 + left / l + right / l + middle / l == (a * b) / p128()
            &&& 0 <= (a * b) / p128() < p128()
        }),
{
    let l = LIMB as int;
    lemma_fundamental_div_mod(a, l);
    lemma_fundamental_div_mod(b, l);
    lemma_mod_bound(a, l);
    lemma_mod_bound(b, l);
    let (a0, a1, b0, b1) = (a % l, a / l, b % l, b / l);
    lemma_div_pos_is_pos(a, l);
    lemma_div_pos_is_pos(b, l);
    assert(a1 < l && b1 < l) by (nonlinear_arith)
        requires
            a == l * a1 + a0,
            b == l * b1 + b0,
            0 <= a0,
            0 <= b0,
            a < l * l,
            b < l * l,
            l > 0,
    ;
    let (low, left, right) = (a0 * b0, a1 * b0, a0 * b1);
    assert(0 <= low && 0 <= left && 0 <= right) by (nonlinear_arith)
        requires
            low == a0 * b0,
            left == a1 * b0,
            right == a0 * b1,
            0 <= a0,
            0 <= a1,
            0 <= b0,
            0 <= b1,
    ;
    lemma_fundamental_div_mod(low, l);
    lemma_fundamental_div_mod(left, l);
    lemma_fundamental_div_mod(right, l);
    lemma_mod_bound(low, l);
    lemma_mod_bound(left, l);
    lemma_mod_bound(right, l);
    let middle = low / l + left % l + right % l;
    lemma_div_pos_is_pos(low, l);
    lemma_fundamental_div_mod(middle, l);
    lemma_mod_bound(middle, l);
    lemma_div_pos_is_pos(middle, l);
    lemma_div_pos_is_pos(left, l);
    lemma_div_pos_is_pos(right, l);
    let high = a1 * b1 + left / l + right / l + middle / l;
    let rest = (middle % l) * l + low % l;
    // The schoolbook expansion, one limb level at a time.
    lemma_two_limb_product(l, a0, a1, b0, b1);
    let (lq, lr, rq, rr) = (left / l, left % l, right / l, right % l);
    let (wq, wr) = (low / l, low % l);
    assert(l * (left + right) + low == l * l * (lq + rq) + l * middle + wr) by (nonlinear_arith)
        requires
            left == l * lq + lr,
            right == l * rq + rr,
            low == l * wq + wr,
            middle == wq + lr + rr,
    ;
    let (mq, mr) = (middle / l, middle % l);
    assert(l * middle + wr == l * l * mq + rest) by (nonlinear_arith)
        requires
            middle == l * mq + mr,
            rest == mr * l + wr,
    ;
    assert(l * l * (a1 * b1) + l * l * (lq + rq) + l * l * mq == high * (l * l)) by (nonlinear_arith)
        requires
            high == a1 * b1 + lq + rq + mq,
    ;
    assert(0 <= rest < l * l) by (nonlinear_arith)
        requires
            0 <= middle % l < l,
            0 <= low % l < l,
            rest == (middle % l) * l + low % l,
    ;
    lemma_fundamental_div_mod_converse(a * b, l * l, high, rest);
    assert(a * b < p128() * p128()) by (nonlinear_arith)
        requires
            0 <= a < p128(),
            0 <= b < p128(),
    ;
    assert(0 <= high < p128()) by (nonlinear_arith)
        requires
            a * b == high * p128() + rest,
            0 <= rest,
            a * b < p128() * p128(),
            0 <= a1 * b1,
            0 <= left / l,
            0 <= right / l,
            0 <= middle / l,
            high == a1 * b1 + left / l + right / l + middle / l,
            p128() > 0,
    ;
}

/// `(l*a1 + a0) * (l*b1 + b0)` expanded by limb, for any limb size `l`.
proof fn lemma_two_limb_product(l: int, a0: int, a1: int, b0: int, b1: int)
    ensures
        (l * a1 + a0) * (l * b1 + b0) == l * l * (a1 * b1) + l * (a1 * b0 + a0 * b1) + a0 * b0,
{
    let (x, hi_b, lo_b) = (l * a1 + a0, l * b1, b0);
    lemma_mul_is_distributive_add(x, hi_b, lo_b);
    lemma_mul_is_distributive_add_other_way(hi_b, l * a1, a0);
    lemma_mul_is_distributive_add_other_way(lo_b, l * a1, a0);
    // (l*a1)*(l*b1) == l*l*(a1*b1), a0*(l*b1) == l*(a0*b1), (l*a1)*b0 == l*(a1*b0).
    assert((l * a1) * (l * b1) == l * l * (a1 * b1)) by (nonlinear_arith);
    assert(a0 * (l * b1) == l * (a0 * b1)) by (nonlinear_arith);
    assert((l * a1) * b0 == l * (a1 * b0)) by (nonlinear_arith);
    lemma_mul_is_distributive_add(l, a1 * b0, a0 * b1);
}

/// With `m = floor((p - 1) / d)`, the estimate `e = floor(n * m / p)` of
/// `floor(n / d)` is never above it and at most one below it, so
/// `n - e * d` lies in `[0, 2d)` and one comparison recovers the quotient.
proof fn lemma_reciprocal_estimate(n: int, d: int, m: int, e: int, p: int)
    requires
        0 < p,
        2 <= d,
        0 <= n < p,
        m == (p - 1) / d,
        e == (n * m) / p,
    ensures
        0 <= e,
        e * d <= n,
        n < e * d + 2 * d,
{
    lemma_fundamental_div_mod(p - 1, d);
    lemma_mod_bound(p - 1, d);
    let r0 = (p - 1) % d;
    assert(m * d <= p - 1 && m * d >= p - d) by (nonlinear_arith)
        requires
            p - 1 == d * m + r0,
            0 <= r0 < d,
    ;
    assert(m >= 0) by (nonlinear_arith)
        requires
            m * d >= p - d,
            d >= 2,
            p > 0,
            p - 1 == d * m + r0,
            0 <= r0 < d,
    ;
    assert(n * m >= 0) by (nonlinear_arith)
        requires
            n >= 0,
            m >= 0,
    ;
    lemma_fundamental_div_mod(n * m, p);
    lemma_mod_bound(n * m, p);
    lemma_div_pos_is_pos(n * m, p);
    let r1 = (n * m) % p;
    // Upper bound: e * d <= n.
    assert(e * p * d <= n * m * d) by (nonlinear_arith)
        requires
            n * m == p * e + r1,
            0 <= r1,
            d >= 0,
    ;
    assert(n * m * d <= n * (p - 1)) by (nonlinear_arith)
        requires
            m * d <= p - 1,
            n >= 0,
    ;
    assert((e * d) * p <= n * p) by (nonlinear_arith)
        requires
            e * p * d <= n * (p - 1),
            n >= 0,
    ;
    assert(e * d <= n) by (nonlinear_arith)
        requires
            (e * d) * p <= n * p,
            p > 0,
    ;
    // Lower bound: n < (e + 2) * d.
    assert(n * m * d >= n * (p - d)) by (nonlinear_arith)
        requires
            m * d >= p - d,
            n >= 0,
    ;
    assert(n * m * d < (e + 1) * p * d) by (nonlinear_arith)
        requires
            n * m == p * e + r1,
            r1 < p,
            d > 0,
    ;
    assert(n * d < p * d) by (nonlinear_arith)
        requires
            n < p,
            d > 0,
    ;
    assert((n - d) * p < ((e + 1) * d) * p) by (nonlinear_arith)
        requires
            n * m * d >= n * (p - d),
            n * m * d < (e + 1) * p * d,
            n * d < p * d,
    ;
    assert(n - d < (e + 1) * d) by (nonlinear_arith)
        requires
            (n - d) * p < ((e + 1) * d) * p,
            p > 0,
    ;
    assert((e + 1) * d == e * d + d) by (nonlinear_arith);
}

/// `x <= d` keeps `floor(x * u64::MAX / d)` within `u64`.
proof fn lemma_quotient_fits(x: int, d: int)
    requires
        0 <= x <= d,
        d >= 1,
    ensures
        0 <= (x * MAX_CODE as int) / d <= MAX_CODE as int,
{
    let m = MAX_CODE as int;
    assert(0 <= x * m <= d * m) by (nonlinear_arith)
        requires
            0 <= x <= d,
            m >= 0,
    ;
    lemma_div_is_ordered(x * m, d * m, d);
    lemma_div_pos_is_pos(x * m, d);
    lemma_fundamental_div_mod_converse(d * m, d, m, 0);
}

proof fn lemma_zero_shr(s: u128)
    ensures
        0u128 >> s == 0,
{
    assert(0u128 >> s == 0) by (bit_vector);
}

proof fn lemma_shr_zero(x: u128)
    ensures
        x >> 0u128 == x,
{
    assert(x >> 0u128 == x) by (bit_vector);
}

proof fn lemma_shr_step(x: u128, s: u128)
    requires
        s < 64,
    ensures
        (x >> s) >> 1u128 == x >> (s + 1) as u128,
{
    assert(s < 64 ==> (x >> s) >> 1u128 == x >> add(s, 1u128)) by (bit_vector);
}

/// Shifting right by 64 or more always lands within a `u64`, so a span still
/// wider than that has been shifted by less than 64.
proof fn lemma_shr_wide_means_short_shift(x: u128, s: u128)
    requires
        s <= 64,
        x >> s > 0xffff_ffff_ffff_ffffu128,
    ensures
        s < 64,
{
    assert(s <= 64 && x >> s > 0xffff_ffff_ffff_ffffu128 ==> s < 64) by (bit_vector);
}

proof fn lemma_half_of_wide_is_positive(x: u128)
    requires
        x > 0xffff_ffff_ffff_ffffu128,
    ensures
        x >> 1u128 >= 1,
{
    assert(x > 0xffff_ffff_ffff_ffffu128 ==> x >> 1u128 >= 1) by (bit_vector);
}

proof fn lemma_shr_monotone(a: u128, b: u128, s: u128)
    requires
        a <= b,
    ensures
        a >> s <= b >> s,
{
    assert(a <= b ==> a >> s <= b >> s) by (bit_vector);
}

} // verus!

#[cfg(test)]
mod tests {
    use super::Rescaler;

    /// The definition the reciprocal arithmetic must reproduce, computed with
    /// plain `u128` division.
    fn by_division(key: u128, lo: u128, hi: u128) -> u64 {
        if hi <= lo {
            return 0;
        }
        let span = hi - lo;
        let shift = span.bit_width().saturating_sub(u64::BITS);
        let x = (key.clamp(lo, hi) - lo) >> shift;
        let quotient = x * u128::from(u64::MAX) / (span >> shift);
        u64::try_from(quotient).expect("quotient fits in u64")
    }

    #[test]
    fn rescales_the_range_ends_and_clamps_outside_it() {
        let rescaler = Rescaler::new(10, 20);
        assert_eq!(rescaler.rescale(10), 0);
        assert_eq!(rescaler.rescale(20), u64::MAX);
        assert_eq!(rescaler.rescale(0), 0);
        assert_eq!(rescaler.rescale(u128::MAX), u64::MAX);
        assert_eq!(rescaler.rescale(15), u64::MAX / 2);
    }

    #[test]
    fn constant_and_inverted_ranges_map_to_zero() {
        for (lo, hi) in [(5, 5), (7, 3), (u128::MAX, 0)] {
            let rescaler = Rescaler::new(lo, hi);
            for key in [0, 3, 5, 7, u128::MAX] {
                assert_eq!(rescaler.rescale(key), 0, "lo={lo} hi={hi} key={key}");
            }
        }
    }

    /// A span of `2^64` is shifted by one bit before dividing, so key 3
    /// rescales to `floor(1 * u64::MAX / 2^63) = 1`, not the unshifted 2.
    #[test]
    fn wide_spans_divide_the_shifted_operands() {
        let rescaler = Rescaler::new(0, 1 << 64);
        assert_eq!(rescaler.rescale(3), 1);
        assert_eq!(by_division(3, 0, 1 << 64), 1);
    }

    /// Compares against plain division over seeded random ranges, including
    /// every span width and keys at and around the range ends.
    #[test]
    fn matches_division_over_random_ranges() {
        const SEED: u64 = 0x5eed_1e55_c0de_d00d;
        let mut state = SEED;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for width in 0..=128u32 {
            for _ in 0..64 {
                let wide = (u128::from(next()) << 64) | u128::from(next());
                let span = if width == 128 {
                    wide
                } else {
                    wide & ((1u128 << width) - 1)
                };
                let lo = u128::from(next()) & (u128::MAX - span);
                let hi = lo + span;
                let rescaler = Rescaler::new(lo, hi);
                let inside = if span == 0 {
                    lo
                } else {
                    lo + (wide % (span + 1))
                };
                for key in [
                    lo,
                    hi,
                    inside,
                    lo.saturating_sub(1),
                    hi.saturating_add(1),
                    0,
                    u128::MAX,
                ] {
                    assert_eq!(
                        rescaler.rescale(key),
                        by_division(key, lo, hi),
                        "seed={SEED:#x} lo={lo} hi={hi} key={key}"
                    );
                }
            }
        }
    }
}
