//! UVTools-style empty first layer.
//!
//! Some printers ignore the wait times (and other per-layer settings) on the very
//! first layer of a file. The workaround UVTools applies is to prepend one
//! throwaway layer that cures nothing, so the printer's first-layer handling lands
//! on that layer and the real first layer is layer 2 as far as the firmware is
//! concerned.
//!
//! The inserted layer, matching a file UVTools produced for a Whale3 SE:
//!   - sits at the **same Z** as the real first layer, so the print does not get
//!     one layer height taller and no layer changes thickness,
//!   - exposes for 0.01 s at the lowest PWM, with one lit pixel, so nothing cures
//!     but the layer is not an empty image that firmware might skip,
//!   - has no light-off delay and no wait times at all,
//!   - lifts 0.1 mm and comes straight back, at the speeds the real first layer
//!     would have used,
//!   - counts as one more bottom layer in the header, so the real bottom layers
//!     are still treated as bottom layers.
//!
//! Because the layer is prepended, every real layer moves one position down the
//! file. CTB v5 obfuscates each layer's RLE with a key derived from its position,
//! so the real layers' data has to be re-keyed from their source position to their
//! new file position (`rekey_layer_rle`).

use super::ctb_first_layer::first_layer_motion;
use super::ctb_layout::{ctb_layer_rle_xor, push_ctb_run};
use super::ctb_types::{CtbPreparedLayer, CtbTimingModel};

/// Exposure written for the inserted layer. Not zero: a zero-exposure layer is
/// treated as a skipped layer by some firmware.
pub(super) const EMPTY_FIRST_LAYER_EXPOSURE_SEC: f32 = 0.01;
/// Lift and retract distance for the inserted layer, mm.
pub(super) const EMPTY_FIRST_LAYER_LIFT_MM: f32 = 0.1;
/// Light PWM for the inserted layer (1/255 of full power).
pub(super) const EMPTY_FIRST_LAYER_PWM: u16 = 1;
/// Grey value of the single lit pixel in the inserted layer.
const EMPTY_FIRST_LAYER_PIXEL_VALUE: u8 = 128;
/// Longest run `push_ctb_run` can encode in one packet.
const MAX_RUN_LEN: usize = 0x0fff_ffff;

/// 1 when a blank layer is prepended, 0 otherwise. Added to the header's layer and
/// bottom-layer counts, and subtracted when the header's total height is derived
/// from the layer count.
pub(super) fn empty_layer_count(timing: &CtbTimingModel) -> u32 {
    if timing.empty_first_layer {
        1
    } else {
        0
    }
}

/// Lift distance and speeds for the inserted layer: a short hop at whatever speeds
/// the real first layer uses (its first-layer movement mode, or the bottom speeds).
pub(super) fn empty_first_layer_motion(
    layer_height_mm: f32,
    timing: &CtbTimingModel,
) -> (f32, f32, f32) {
    let (lift_speed, retract_speed) = match first_layer_motion(0, layer_height_mm, timing) {
        Some(m) => (m.lift_speed_mm_min, m.retract_speed_mm_min),
        None => (
            timing.bottom_lift_speed_mm_min,
            timing.bottom_retract_speed_mm_min,
        ),
    };
    (EMPTY_FIRST_LAYER_LIFT_MM, lift_speed, retract_speed)
}

/// Emits `len` pixels of `value`, splitting runs longer than one CTB packet can hold.
fn push_run_chunked(out: &mut Vec<u8>, len: usize, value: u8) {
    let mut left = len;
    while left > 0 {
        let chunk = left.min(MAX_RUN_LEN);
        push_ctb_run(out, chunk as u32, value);
        left -= chunk;
    }
}

/// The layer that gets prepended: a blank image with one lit pixel in the middle.
///
/// `width_px` / `height_px` are the printer's resolution, so the image has exactly
/// as many pixels as the header advertises. The RLE is obfuscated for file
/// position 0, and `index` stays 0 so that every per-layer rule (Z position, first
/// layer movement) sees it as the first layer; the values it is actually written
/// with are overridden wholesale in `write_layer_def_ex`.
pub(super) fn build_empty_first_layer(
    width_px: u32,
    height_px: u32,
    layer_xor_key: u32,
) -> CtbPreparedLayer {
    let pixels = (width_px as usize).saturating_mul(height_px as usize);
    let centre = if pixels > 0 {
        (height_px as usize / 2).saturating_mul(width_px as usize) + width_px as usize / 2
    } else {
        0
    };
    let centre = centre.min(pixels.saturating_sub(1));

    let mut encoded = Vec::with_capacity(16);
    if pixels > 0 {
        push_run_chunked(&mut encoded, centre, 0);
        push_ctb_run(&mut encoded, 1, EMPTY_FIRST_LAYER_PIXEL_VALUE);
        push_run_chunked(&mut encoded, pixels - centre - 1, 0);
    }
    ctb_layer_rle_xor(layer_xor_key, 0, &mut encoded);

    CtbPreparedLayer {
        index: 0,
        source_len: pixels,
        encoded,
    }
}

/// Moves already-obfuscated layer data from one file position to another.
///
/// The obfuscation is a keystream XOR, so un-keying the old position and keying the
/// new one is two passes of the same function. A no-op when the position has not
/// changed or the file is not obfuscated.
pub(super) fn rekey_layer_rle(seed: u32, from_index: u32, to_index: u32, bytes: &mut [u8]) {
    if seed == 0 || from_index == to_index {
        return;
    }
    ctb_layer_rle_xor(seed, from_index, bytes);
    ctb_layer_rle_xor(seed, to_index, bytes);
}

#[cfg(test)]
mod tests {
    use super::super::ctb_metadata::parse_timing_model_from_metadata;
    use super::super::ctb_types::CtbFirstLayerMove;
    use super::*;

    /// Decodes a CTB RLE packet stream into (length, value) runs.
    fn decode_runs(data: &[u8]) -> Vec<(usize, u8)> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < data.len() {
            let code = data[i];
            i += 1;
            let value = (code & 0x7f) << 1;
            if code & 0x80 == 0 {
                out.push((1, value));
                continue;
            }
            let n = data[i];
            i += 1;
            let len = if n & 0x80 == 0 {
                n as usize
            } else if n & 0xc0 == 0x80 {
                let v = (((n & 0x3f) as usize) << 8) | data[i] as usize;
                i += 1;
                v
            } else if n & 0xe0 == 0xc0 {
                let v = (((n & 0x1f) as usize) << 16)
                    | ((data[i] as usize) << 8)
                    | data[i + 1] as usize;
                i += 2;
                v
            } else {
                let v = (((n & 0x1f) as usize) << 24)
                    | ((data[i] as usize) << 16)
                    | ((data[i + 1] as usize) << 8)
                    | data[i + 2] as usize;
                i += 3;
                v
            };
            out.push((len, value));
        }
        out
    }

    #[test]
    fn empty_layer_is_blank_except_one_middle_pixel() {
        let layer = build_empty_first_layer(7680, 4320, 0);
        let runs = decode_runs(&layer.encoded);
        let pixels: usize = runs.iter().map(|r| r.0).sum();
        assert_eq!(pixels, 7680 * 4320, "must cover the whole screen");
        assert_eq!(layer.source_len, 7680 * 4320);
        assert_eq!(layer.index, 0);

        let lit: Vec<_> = runs.iter().filter(|r| r.1 != 0).collect();
        assert_eq!(lit, vec![&(1_usize, 128_u8)], "exactly one lit pixel");
        // That pixel sits in the middle of the screen.
        let before: usize = runs.iter().take_while(|r| r.1 == 0).map(|r| r.0).sum();
        assert_eq!(before, 4320 / 2 * 7680 + 7680 / 2);
    }

    #[test]
    fn empty_layer_is_obfuscated_for_file_position_zero() {
        let plain = build_empty_first_layer(64, 32, 0);
        let keyed = build_empty_first_layer(64, 32, 0xEFBE_ADDE);
        assert_ne!(plain.encoded, keyed.encoded);

        let mut undone = keyed.encoded.clone();
        ctb_layer_rle_xor(0xEFBE_ADDE, 0, &mut undone);
        assert_eq!(undone, plain.encoded);
    }

    #[test]
    fn rekey_moves_data_between_file_positions() {
        let seed = 0xEFBE_ADDE;
        let plain = vec![1_u8, 2, 3, 4, 5, 6, 7, 8, 9];

        // A layer keyed for position 3, moved to position 4, matches one keyed for 4.
        let mut at_3 = plain.clone();
        ctb_layer_rle_xor(seed, 3, &mut at_3);
        rekey_layer_rle(seed, 3, 4, &mut at_3);
        let mut at_4 = plain.clone();
        ctb_layer_rle_xor(seed, 4, &mut at_4);
        assert_eq!(at_3, at_4);

        // No seed or no move: untouched.
        let mut same = plain.clone();
        rekey_layer_rle(seed, 2, 2, &mut same);
        assert_eq!(same, plain);
        let mut unkeyed = plain.clone();
        rekey_layer_rle(0, 1, 2, &mut unkeyed);
        assert_eq!(unkeyed, plain);
    }

    #[test]
    fn empty_layer_count_follows_the_setting() {
        let mut t = parse_timing_model_from_metadata("{}");
        assert_eq!(empty_layer_count(&t), 0);
        t.empty_first_layer = true;
        assert_eq!(empty_layer_count(&t), 1);
    }

    #[test]
    fn empty_layer_motion_follows_the_first_layer_speeds() {
        let mut t = parse_timing_model_from_metadata("{}");
        t.bottom_lift_speed_mm_min = 60.0;
        t.bottom_retract_speed_mm_min = 90.0;
        assert_eq!(empty_first_layer_motion(0.05, &t), (0.1, 60.0, 90.0));

        // With a Whale3 first-layer movement mode, the slow 20 mm/min speeds apply.
        t.first_layer_move = CtbFirstLayerMove::Home;
        assert_eq!(empty_first_layer_motion(0.05, &t), (0.1, 20.0, 20.0));
    }
}
