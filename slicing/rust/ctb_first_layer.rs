//! Whale3-style first-layer movement, the same two modes as the Whale3 GCode
//! Generator's CWS output:
//!
//!   - Below home: lift 2.5 mm, retract 2.5 mm plus one layer height, so layer 1
//!     cures one layer height below home (works around Chituboards placing the
//!     first layer one layer too high).
//!   - Home: lift 2.5 mm, retract 2.5 mm, so layer 1 cures exactly at home.
//!
//! Both moves run at 20 mm/min. In a CTB every layer has an absolute Z, so the
//! whole stack is moved down with layer 1 (one layer height for Home, two for
//! Below home), exactly as the relative CWS moves do. Layer thicknesses stay the
//! same. Layer 1's own lift values carry the slow 2.5 mm move.

use super::ctb_types::{CtbFirstLayerMove, CtbTimingModel};

/// Lift before layer 1, mm.
pub(super) const FIRST_LAYER_LIFT_MM: f32 = 2.5;
/// Lift and retract speed for that move, mm/min.
pub(super) const FIRST_LAYER_SPEED_MM_MIN: f32 = 20.0;

/// Lift/retract values written into layer 1 instead of the bottom values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct FirstLayerMotion {
    pub lift_mm: f32,
    pub lift_speed_mm_min: f32,
    pub retract_mm: f32,
    pub retract_speed_mm_min: f32,
}

/// How far the whole stack moves down compared with the default placement.
fn stack_offset_layers(timing: &CtbTimingModel) -> f32 {
    match timing.first_layer_move {
        CtbFirstLayerMove::Default => 0.0,
        CtbFirstLayerMove::Home => 1.0,
        CtbFirstLayerMove::BelowHome => 2.0,
    }
}

/// Absolute Z written into the layer table for the layer at `index` (0-based).
/// Default: (index + 1) * layer height. Home: layer 1 at 0. Below home: layer 1 at -layer height.
pub(super) fn layer_position_z_mm(index: usize, layer_height_mm: f32, timing: &CtbTimingModel) -> f32 {
    (index as f32 + 1.0 - stack_offset_layers(timing)) * layer_height_mm
}

/// Layer 1's lift/retract when a first-layer mode is on; `None` for every other layer
/// and in default mode.
pub(super) fn first_layer_motion(
    index: usize,
    layer_height_mm: f32,
    timing: &CtbTimingModel,
) -> Option<FirstLayerMotion> {
    if index != 0 || timing.first_layer_move == CtbFirstLayerMove::Default {
        return None;
    }
    // The plate starts at home (0), goes up 2.5 mm, then down to layer 1's Z.
    let target = layer_position_z_mm(0, layer_height_mm, timing);
    Some(FirstLayerMotion {
        lift_mm: FIRST_LAYER_LIFT_MM,
        lift_speed_mm_min: FIRST_LAYER_SPEED_MM_MIN,
        retract_mm: FIRST_LAYER_LIFT_MM - target,
        retract_speed_mm_min: FIRST_LAYER_SPEED_MM_MIN,
    })
}

#[cfg(test)]
mod tests {
    use super::super::ctb_metadata::parse_timing_model_from_metadata;
    use super::*;

    const LH: f32 = 0.05;

    fn timing(mode: CtbFirstLayerMove) -> CtbTimingModel {
        let mut t = parse_timing_model_from_metadata("{}");
        t.first_layer_move = mode;
        t
    }

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-6
    }

    #[test]
    fn default_mode_keeps_positions_and_motion() {
        let t = timing(CtbFirstLayerMove::Default);
        assert!(close(layer_position_z_mm(0, LH, &t), 0.05));
        assert!(close(layer_position_z_mm(9, LH, &t), 0.5));
        assert_eq!(first_layer_motion(0, LH, &t), None);
    }

    #[test]
    fn home_mode_cures_layer_1_at_home() {
        let t = timing(CtbFirstLayerMove::Home);
        assert!(close(layer_position_z_mm(0, LH, &t), 0.0));
        assert!(close(layer_position_z_mm(1, LH, &t), 0.05));
        let m = first_layer_motion(0, LH, &t).unwrap();
        assert!(close(m.lift_mm, 2.5) && close(m.retract_mm, 2.5));
        assert!(close(m.lift_speed_mm_min, 20.0) && close(m.retract_speed_mm_min, 20.0));
        assert_eq!(first_layer_motion(1, LH, &t), None);
    }

    #[test]
    fn below_home_mode_cures_layer_1_one_layer_below_home() {
        let t = timing(CtbFirstLayerMove::BelowHome);
        assert!(close(layer_position_z_mm(0, LH, &t), -0.05));
        assert!(close(layer_position_z_mm(1, LH, &t), 0.0));
        assert!(close(layer_position_z_mm(2, LH, &t), 0.05));
        let m = first_layer_motion(0, LH, &t).unwrap();
        assert!(close(m.lift_mm, 2.5) && close(m.retract_mm, 2.55));
    }

    #[test]
    fn metadata_selects_the_mode() {
        let parse = |v: &str| {
            parse_timing_model_from_metadata(&format!(r#"{{"ctb":{{"firstLayerMoveMode":"{v}"}}}}"#))
                .first_layer_move
        };
        assert_eq!(parse("below-home"), CtbFirstLayerMove::BelowHome);
        assert_eq!(parse("home"), CtbFirstLayerMove::Home);
        assert_eq!(parse("default"), CtbFirstLayerMove::Default);
        assert_eq!(parse("anything else"), CtbFirstLayerMove::Default);
        assert_eq!(
            parse_timing_model_from_metadata("{}").first_layer_move,
            CtbFirstLayerMove::Default
        );
    }
}
