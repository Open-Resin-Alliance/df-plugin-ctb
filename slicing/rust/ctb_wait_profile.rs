//! Per-layer "wait before cure" (rest after retract) schedule.
//!
//! Without a wait profile, the CTB encoder uses a two-tier rule: layers below
//! `wait_time_bottom_layer_count` get `bottom_wait_time_before_cure_sec`, every
//! other layer gets `wait_time_before_cure_sec`.
//!
//! With a wait profile (`CtbTimingModel::wait_profile`), the schedule is:
//!   1. layer 1 / layer 2 use their own value when set,
//!   2. bottom layers use the bottom wait,
//!   3. layers whose Z is at or below the hold height use the bottom wait,
//!   4. above the hold height, the wait steps down each layer (by time or over a
//!      distance) until it reaches the normal wait, then stays there.
//!
//! This is a pure function of (layer index, layer height, timing), so the layer
//! writer and the print-time estimate always agree.

use super::ctb_types::{CtbTimingModel, CtbWaitTransition};

/// Tolerance used when comparing a layer's Z with the hold height, so that a
/// layer sitting exactly on the hold height is not lost to float rounding.
const Z_EPSILON_MM: f32 = 1e-4;

/// Z position of a layer, matching the value written into the layer table.
fn layer_z_mm(index: usize, layer_height_mm: f32) -> f32 {
    (index as f32 + 1.0) * layer_height_mm
}

fn is_at_or_below_hold(index: usize, layer_height_mm: f32, hold_height_mm: f32) -> bool {
    layer_z_mm(index, layer_height_mm) <= hold_height_mm + Z_EPSILON_MM
}

/// Index of the first layer whose Z is above the hold height.
fn first_index_above_hold(layer_height_mm: f32, hold_height_mm: f32) -> usize {
    if !(layer_height_mm > 0.0) {
        return 0;
    }
    // Estimate, then settle with the exact same predicate the per-layer test uses.
    let mut i = ((hold_height_mm + Z_EPSILON_MM) / layer_height_mm).floor().max(0.0) as usize;
    while i > 0 && !is_at_or_below_hold(i - 1, layer_height_mm, hold_height_mm) {
        i -= 1;
    }
    while is_at_or_below_hold(i, layer_height_mm, hold_height_mm) {
        i += 1;
    }
    i
}

fn round_centiseconds(value: f32) -> f32 {
    (value * 100.0).round() / 100.0
}

/// True when the layer uses the "extended" (bottom) values for wait after cure
/// and wait after lift. Unchanged from the encoder's original rule.
pub(super) fn is_extended_wait_layer(index: usize, timing: &CtbTimingModel) -> bool {
    (index as u32) < timing.wait_time_bottom_layer_count
}

/// Wait before cure, in seconds, for the layer at `index` (0-based).
pub(super) fn wait_before_cure_sec(
    index: usize,
    layer_height_mm: f32,
    timing: &CtbTimingModel,
) -> f32 {
    let Some(profile) = timing.wait_profile else {
        // Original two-tier behaviour.
        return if is_extended_wait_layer(index, timing) {
            timing.bottom_wait_time_before_cure_sec
        } else {
            timing.wait_time_before_cure_sec
        };
    };

    if index == 0 {
        if let Some(v) = profile.layer1_wait_sec {
            return round_centiseconds(v);
        }
    }
    if index == 1 {
        if let Some(v) = profile.layer2_wait_sec {
            return round_centiseconds(v);
        }
    }

    let bottom = timing.bottom_wait_time_before_cure_sec;
    let normal = timing.wait_time_before_cure_sec;
    // Same layer count as the other two waits, so all three switch over together.
    let bottom_count = timing.wait_time_bottom_layer_count as usize;

    if index < bottom_count {
        return round_centiseconds(bottom);
    }
    if profile.hold_height_mm <= 0.0 {
        return round_centiseconds(normal);
    }
    if is_at_or_below_hold(index, layer_height_mm, profile.hold_height_mm) {
        return round_centiseconds(bottom);
    }

    // Transition: the first layer above both the bottom layers and the hold
    // height is step 1.
    let ramp_start = bottom_count.max(first_index_above_hold(layer_height_mm, profile.hold_height_mm));
    let steps_taken = (index + 1).saturating_sub(ramp_start) as f32;

    let step_sec = match profile.transition {
        CtbWaitTransition::TimePerLayer { decrease_sec } => decrease_sec,
        CtbWaitTransition::Distance { distance_mm } => {
            let layers_over = if layer_height_mm > 0.0 {
                (distance_mm / layer_height_mm).round().max(1.0)
            } else {
                1.0
            };
            (bottom - normal) / layers_over
        }
    };

    if !(step_sec > 0.0) {
        // No usable step: switch straight to the normal wait.
        return round_centiseconds(normal);
    }

    round_centiseconds((bottom - step_sec * steps_taken).max(normal))
}

#[cfg(test)]
mod tests {
    use super::super::ctb_metadata::parse_timing_model_from_metadata;
    use super::super::ctb_types::{CtbWaitProfile, CtbWaitTransition};
    use super::*;

    const LH: f32 = 0.05;

    fn timing_with(profile: Option<CtbWaitProfile>, bottom_layers: u32, normal: f32) -> CtbTimingModel {
        let mut t = parse_timing_model_from_metadata("{}");
        t.bottom_layer_count = bottom_layers;
        t.wait_time_bottom_layer_count = bottom_layers;
        t.wait_time_before_cure_sec = normal;
        t.wait_profile = profile;
        t
    }

    /// Timing with a profile and an explicit bottom wait before cure.
    fn timing_with_bottom(
        profile: CtbWaitProfile,
        bottom_layers: u32,
        normal: f32,
        bottom_wait: f32,
    ) -> CtbTimingModel {
        let mut t = timing_with(Some(profile), bottom_layers, normal);
        t.bottom_wait_time_before_cure_sec = bottom_wait;
        t
    }

    fn profile(
        layer1: Option<f32>,
        layer2: Option<f32>,
        hold: f32,
        transition: CtbWaitTransition,
    ) -> CtbWaitProfile {
        CtbWaitProfile {
            layer1_wait_sec: layer1,
            layer2_wait_sec: layer2,
            hold_height_mm: hold,
            transition,
        }
    }

    fn schedule(n: usize, t: &CtbTimingModel) -> Vec<f32> {
        (0..n).map(|i| wait_before_cure_sec(i, LH, t)).collect()
    }

    #[test]
    fn no_profile_keeps_two_tier_rule() {
        let mut t = timing_with(None, 3, 2.0);
        t.bottom_wait_time_before_cure_sec = 8.0;
        assert_eq!(schedule(5, &t), vec![8.0, 8.0, 8.0, 2.0, 2.0]);
    }

    #[test]
    fn layer1_and_layer2_override_then_bottom_then_normal() {
        let p = profile(Some(600.0), Some(300.0), 0.0, CtbWaitTransition::TimePerLayer { decrease_sec: 1.0 });
        let t = timing_with_bottom(p, 4, 1.5, 10.0);
        assert_eq!(schedule(6, &t), vec![600.0, 300.0, 10.0, 10.0, 1.5, 1.5]);
    }

    #[test]
    fn unset_layer_overrides_fall_back_to_bottom_wait() {
        let p = profile(None, None, 0.0, CtbWaitTransition::TimePerLayer { decrease_sec: 1.0 });
        let t = timing_with_bottom(p, 2, 1.0, 10.0);
        assert_eq!(schedule(3, &t), vec![10.0, 10.0, 1.0]);
    }

    #[test]
    fn hold_height_then_time_based_ramp() {
        // Bottom layers 0..2; hold to 0.25 mm (layers 0..4 at z<=0.25); then -2 s/layer down to 3 s.
        let p = profile(None, None, 0.25, CtbWaitTransition::TimePerLayer { decrease_sec: 2.0 });
        let t = timing_with_bottom(p, 2, 3.0, 10.0);
        assert_eq!(schedule(10, &t), vec![10.0, 10.0, 10.0, 10.0, 10.0, 8.0, 6.0, 4.0, 3.0, 3.0]);
    }

    #[test]
    fn hold_height_then_distance_based_ramp() {
        // Hold to 0.1 mm (layers 0,1); drop 10 -> 2 over 0.2 mm = 4 layers, 2 s each.
        let p = profile(None, None, 0.1, CtbWaitTransition::Distance { distance_mm: 0.2 });
        let t = timing_with_bottom(p, 1, 2.0, 10.0);
        assert_eq!(schedule(8, &t), vec![10.0, 10.0, 8.0, 6.0, 4.0, 2.0, 2.0, 2.0]);
    }

    #[test]
    fn ramp_starts_after_bottom_layers_when_they_exceed_hold_height() {
        // Hold height (0.05 mm) is inside the 4 bottom layers; ramp starts at layer index 4.
        let p = profile(None, None, 0.05, CtbWaitTransition::TimePerLayer { decrease_sec: 3.0 });
        let t = timing_with_bottom(p, 4, 1.0, 9.0);
        assert_eq!(schedule(8, &t), vec![9.0, 9.0, 9.0, 9.0, 6.0, 3.0, 1.0, 1.0]);
    }

    #[test]
    fn zero_step_switches_straight_to_normal() {
        let p = profile(None, None, 0.1, CtbWaitTransition::TimePerLayer { decrease_sec: 0.0 });
        let t = timing_with_bottom(p, 1, 1.0, 9.0);
        assert_eq!(schedule(4, &t), vec![9.0, 9.0, 1.0, 1.0]);
    }

    #[test]
    fn layer_on_hold_boundary_counts_as_held() {
        // 100 layers of 0.05 mm = exactly 5.0 mm; layer index 99 sits on the hold height.
        let p = profile(None, None, 5.0, CtbWaitTransition::TimePerLayer { decrease_sec: 1.0 });
        let t = timing_with_bottom(p, 1, 1.0, 10.0);
        assert_eq!(wait_before_cure_sec(99, LH, &t), 10.0);
        assert_eq!(wait_before_cure_sec(100, LH, &t), 9.0);
    }

    #[test]
    fn matches_whale3_wait_editor_example() {
        // Same shape as the Whale3 CTB Wait Editor: L1 600 s, L2 300 s, bottom 60 s held to
        // 1 mm, then -5 s/layer down to 2 s. 0.05 mm layers, 6 bottom layers.
        let p = profile(Some(600.0), Some(300.0), 1.0, CtbWaitTransition::TimePerLayer { decrease_sec: 5.0 });
        let t = timing_with_bottom(p, 6, 2.0, 60.0);
        let s = schedule(40, &t);
        assert_eq!(&s[..3], &[600.0, 300.0, 60.0]);
        assert_eq!(s[19], 60.0); // z = 1.00 mm, still held
        assert_eq!(s[20], 55.0); // first ramp layer
        assert_eq!(s[30], 5.0);
        assert_eq!(s[31], 2.0); // clamped at normal
        assert_eq!(s[39], 2.0);
    }
}
