// Non-encrypted CTB V5 format handling
use crate::engine::SlicerV3Error;
use crate::types::SliceJobV3;

use super::ctb_layout::{
    page_number_and_offset, push_bytes_padded, push_f32, push_u16, push_u32, push_u8, push_u64,
};
use super::ctb_metadata::{
    decode_embedded_disclaimer_bytes, parse_ctb_build_model_from_job,
    parse_ctb_resin_model_from_job, parse_machine_software_version,
    parse_timing_model_from_metadata,
};
use super::ctb_preview::build_previews;
use super::ctb_preview::write_preview_record;
use super::ctb_wait_profile::{is_extended_wait_layer, wait_before_cure_sec};
use super::ctb_first_layer::{first_layer_motion, layer_position_z_mm};
use super::ctb_empty_layer::{
    build_empty_first_layer, empty_first_layer_motion, empty_layer_count, rekey_layer_rle,
    EMPTY_FIRST_LAYER_EXPOSURE_SEC, EMPTY_FIRST_LAYER_PWM,
};
use super::ctb_types::{
    CtbBuildModel, CtbExtendedOffsets, CtbPreparedLayer, CtbPreviewOffsets, CtbResinModel,
    CtbResinPayload, CtbTimingModel, CTB_DISCLAIMER_SIZE, CTB_HEADER_SIZE, CTB_LAYER_DEF_EX_SIZE,
    CTB_LAYER_DEF_SIZE, CTB_MAGIC_V2_V3, CTB_MAGIC_V4_V5, CTB_PREVIEW_RECORD_SIZE,
    CTB_PRINT_PARAMETERS_SIZE, CTB_PRINT_PARAMETERS_V4_RESERVED_SIZE, CTB_PRINT_PARAMETERS_V4_SIZE,
    CTB_SLICER_INFO_FIXED_SIZE, CTB_PER_LAYER_OVERHEAD_SEC,
    CTB_ENCRYPTED_LAYER_DEF_SIZE, CTB_ENCRYPTED_SETTINGS_OFFSET, CTB_ENCRYPTED_SETTINGS_SIZE, CTB_MAGIC_V5_ENCRYPTED, CTB_ENCRYPTED_HEADER_SIZE
};
use sha2::{Digest, Sha256};
use super::ctb_crypto::{ctb_default_key_iv, ctb_encrypt_in_place_no_padding};

struct CtbVersionCaps {
    magic: u32,
    extended_layer_def: bool,
    print_params_v4: bool,
    /// V5+: write ResinParameters block and null-terminate all metadata strings.
    resin_params: bool,
    per_layer_settings_active_flag: u8,
}

fn ctb_version_caps(version: u32) -> CtbVersionCaps {
    match version.clamp(2, 5) {
        2 => CtbVersionCaps {
            magic: CTB_MAGIC_V2_V3,
            extended_layer_def: false,
            print_params_v4: false,
            resin_params: false,
            per_layer_settings_active_flag: 0x20,
        },
        3 => CtbVersionCaps {
            magic: CTB_MAGIC_V2_V3,
            extended_layer_def: true,
            print_params_v4: false,
            resin_params: false,
            per_layer_settings_active_flag: 0x30,
        },
        4 => CtbVersionCaps {
            magic: CTB_MAGIC_V4_V5,
            extended_layer_def: true,
            print_params_v4: true,
            resin_params: false,
            per_layer_settings_active_flag: 0x40,
        }, //TODO Uniformation Magic V4
        _ => CtbVersionCaps {
            magic: CTB_MAGIC_V4_V5,
            extended_layer_def: true,
            print_params_v4: true,
            resin_params: true,
            per_layer_settings_active_flag: 0x50,
        },
    }
}

fn write_ctb_header(
    out: &mut Vec<u8>,
    magic: u32,
    version: u32,
    job: &SliceJobV3,
    layer_count: u32,
    timing: CtbTimingModel,
    build: &CtbBuildModel,
    preview_offsets: CtbPreviewOffsets,
    print_parameters_offset: u32,
    layers_definition_offset: u32,
    print_time_sec: u32,
    slicer_offset: u32,
    slicer_size: u32,
) {
    push_u32(out, magic);
    push_u32(out, version);
    push_f32(out, job.build_width_mm);
    push_f32(out, job.build_depth_mm);
    push_f32(out, build.bed_size_z_mm.max(0.0));
    push_u32(out, build.created_date_unix);
    push_u32(out, build.modified_date_unix);
    // The inserted blank layer shares the real first layer's Z, so it adds no height,
    // but it does count as one more bottom layer so the real bottom layers keep theirs.
    push_f32(out, job.layer_height_mm * layer_count.saturating_sub(empty_layer_count(&timing)) as f32);
    push_f32(out, job.layer_height_mm);
    push_f32(out, timing.normal_exposure_sec);
    push_f32(out, timing.bottom_exposure_sec);
    push_f32(out, timing.light_off_delay_sec);
    push_u32(out, timing.bottom_layer_count + empty_layer_count(&timing));
    push_u32(out, job.source_width_px);
    push_u32(out, job.source_height_px);
    push_u32(out, preview_offsets.large_record_offset); //
    push_u32(out, layers_definition_offset); //
    push_u32(out, layer_count);
    push_u32(out, preview_offsets.small_record_offset);
    push_u32(out, print_time_sec);
    push_u32(out, build.projector_type);
    push_u32(out, print_parameters_offset); //
    push_u32(out, CTB_PRINT_PARAMETERS_SIZE); //
    push_u32(out, 1); //
    push_u16(out, timing.projector_duty_cycle_pwm);
    push_u16(out, timing.bottom_layer_projector_duty_cycle_pwm);
    push_u32(out, build.layer_xor_key);
    push_u32(out, slicer_offset); //
    push_u32(out, slicer_size); //
}

fn write_ctb_print_parameters(out: &mut Vec<u8>, timing: CtbTimingModel) {
    // CTB print-parameter lift heights are TOTAL heights (stage1 + stage2).
    // UVTools/ChiTuBox derive stage1 lift height as (total_height - stage2_height).
    let bottom_lift_total_mm = timing.bottom_lift_distance_mm + timing.bottom_lift_distance2_mm;
    let lift_total_mm = timing.lift_distance_mm + timing.lift_distance2_mm;

    push_f32(out, bottom_lift_total_mm.max(0.0));
    push_f32(out, timing.bottom_lift_speed_mm_min);
    push_f32(out, lift_total_mm.max(0.0));
    push_f32(out, timing.lift_speed_mm_min);
    push_f32(out, timing.retract_speed_mm_min);
    push_f32(out, timing.retract_distance_mm);
    push_f32(out, timing.retract_distance2_mm);
    push_f32(out, timing.retract_speed2_mm_min);
    push_f32(out, timing.bottom_light_off_delay_sec);
    push_f32(out, timing.light_off_delay_sec);
    push_u32(out, timing.bottom_layer_count + empty_layer_count(&timing));
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 0);
}

fn write_ctb_slicer_info_fixed(
    out: &mut Vec<u8>,
    build: &CtbBuildModel,
    timing: CtbTimingModel,
    machine_name_offset: u32,
    machine_name_size: u32,
    print_parameters_v4_address: u32,
    per_layer_settings_flag: u8,
) {
    // CTB v4/v5 slicer-info fields (UVTools/ChiTuBox semantics):
    //   [0] BottomLiftHeight2, [1] BottomLiftSpeed2,
    //   [2] LiftHeight2,       [3] LiftSpeed2,
    //   [4] RetractHeight2,    [5] RetractSpeed2,
    //   [6] RestTimeAfterLift.
    push_f32(out, timing.bottom_lift_distance2_mm);
    push_f32(out, timing.bottom_lift_speed2_mm_min);
    push_f32(out, timing.lift_distance2_mm);
    push_f32(out, timing.lift_speed2_mm_min);
    push_f32(out, timing.retract_distance2_mm);
    push_f32(out, timing.retract_speed2_mm_min);
    push_f32(out, timing.wait_time_after_lift_sec);

    push_u32(out, machine_name_offset);
    push_u32(out, machine_name_size);

    push_u8(
        out,
        if build.anti_alias_level > 1 {
            0x0f
        } else {
            0x07
        },
    );
    push_u16(out, 0);

    push_u8(out, per_layer_settings_flag);

    push_u32(out, build.modified_date_unix / 60);
    push_u32(out, build.anti_alias_level);
    push_u32(out, parse_machine_software_version(build.version));

    push_f32(out, timing.wait_time_before_cure_sec);
    push_f32(out, timing.wait_time_after_lift_sec);
    push_u32(out, timing.transition_layer_count);
    push_u32(out, print_parameters_v4_address);
    push_u32(out, 0);
    push_u32(out, 0);
}

fn write_ctb_print_parameters_v4(
    out: &mut Vec<u8>,
    timing: CtbTimingModel,
    layer_count: u32,
    extended_offsets: CtbExtendedOffsets,
) {
    push_f32(out, timing.bottom_retract_speed_mm_min);
    push_f32(out, timing.bottom_retract_speed2_mm_min);
    push_u32(out, 0);
    push_f32(out, timing.retract_distance_mm.max(0.0));
    push_u32(out, 0);
    push_f32(out, timing.retract_distance2_mm.max(0.0));
    push_f32(out, timing.wait_time_before_cure_sec);
    push_f32(out, timing.wait_time_after_lift_sec);
    push_f32(out, timing.wait_time_after_cure_sec);
    push_f32(out, timing.bottom_retract_height2_mm);
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 5);
    push_u32(out, layer_count.saturating_sub(1));
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, extended_offsets.disclaimer_offset);
    push_u32(out, extended_offsets.disclaimer_length);
    push_u32(out, extended_offsets.resin_parameters_offset);
    out.extend_from_slice(&[0u8; CTB_PRINT_PARAMETERS_V4_RESERVED_SIZE]);
}

fn prepare_resin_payload(
    build: &CtbBuildModel,
    resin: &CtbResinModel,
    aes_enabled: bool,
    resin_params: bool,
) -> CtbResinPayload {
    let mut machine_name_bytes = build.machine_name.as_bytes().to_vec();
    let mut resin_type_bytes = resin.resin_type.as_bytes().to_vec();
    let mut resin_name_bytes = resin.resin_name.as_bytes().to_vec();

    if resin_params {
        if !machine_name_bytes.ends_with(&[0]) {
            machine_name_bytes.push(0);
        }
        if !resin_type_bytes.ends_with(&[0]) {
            resin_type_bytes.push(0);
        }
        if !resin_name_bytes.ends_with(&[0]) {
            resin_name_bytes.push(0);
        }
    }

    let base_len =
        40usize + resin_type_bytes.len() + resin_name_bytes.len() + machine_name_bytes.len();
    let tail_padding_bytes = if aes_enabled {
        (16 - (base_len % 16)) % 16
    } else {
        0
    };

    CtbResinPayload {
        machine_name_bytes,
        resin_type_bytes,
        resin_name_bytes,
        tail_padding_bytes,
    }
}

fn resin_payload_len(payload: &CtbResinPayload) -> u32 {
    40 + payload.resin_type_bytes.len() as u32
        + payload.resin_name_bytes.len() as u32
        + payload.machine_name_bytes.len() as u32
        + payload.tail_padding_bytes as u32
}

fn write_ctb_resin_parameters(
    out: &mut Vec<u8>,
    resin_offset: u32,
    payload: &CtbResinPayload,
    resin: &CtbResinModel,
) {
    let fixed_size = 40u32;
    let resin_type_address = resin_offset + fixed_size;
    let resin_name_address = resin_type_address + payload.resin_type_bytes.len() as u32;
    let machine_name_address = resin_name_address + payload.resin_name_bytes.len() as u32;

    push_u32(out, 0);
    push_u8(out, resin.color_rgba[2]);
    push_u8(out, resin.color_rgba[1]);
    push_u8(out, resin.color_rgba[0]);
    push_u8(out, resin.color_rgba[3]);

    push_u32(out, machine_name_address);
    push_u32(out, payload.resin_type_bytes.len() as u32);
    push_u32(out, resin_type_address);
    push_u32(out, payload.resin_name_bytes.len() as u32);
    push_u32(out, resin_name_address);
    push_u32(out, payload.machine_name_bytes.len() as u32);
    push_f32(out, resin.resin_density);
    push_u32(out, 0);

    out.extend_from_slice(&payload.resin_type_bytes);
    out.extend_from_slice(&payload.resin_name_bytes);
    out.extend_from_slice(&payload.machine_name_bytes);
    if payload.tail_padding_bytes != 0 {
        out.resize(out.len() + payload.tail_padding_bytes, 0);
    }
}

enum LayerDefLayout {
    Basic,     // V2: 36-byte def, no extended fields
    Extended,  // V3+: 84-byte def with extended fields
    Encrypted, // All encrypted versions: 88-byte def, different field ordering
}

//TODO generalize function
fn interpolate_transition_layer_exposure(
    layer_index: usize,
    timing: &CtbTimingModel
) -> f32 {
    return timing.bottom_exposure_sec 
        - (timing.bottom_exposure_sec - timing.normal_exposure_sec) 
            / (timing.transition_layer_count as f32) 
            * (layer_index as u32 - timing.bottom_layer_count + 1) as f32;
}

/// Exposure time for a layer, including the bottom -> normal transition layers.
fn layer_exposure_sec(layer_index: usize, is_bottom: bool, timing: &CtbTimingModel) -> f32 {
    if is_bottom {
        timing.bottom_exposure_sec
    } else if (layer_index as u32) < (timing.bottom_layer_count + timing.transition_layer_count) {
        interpolate_transition_layer_exposure(layer_index, timing)
    } else {
        timing.normal_exposure_sec
    }
}

fn write_layer_def_ex(
    out: &mut Vec<u8>,
    layer: &CtbPreparedLayer,
    position_z_mm: f32,
    timing: CtbTimingModel,
    layer_data_abs_offset: u64,
    layout: LayerDefLayout,
    is_bottom: bool,
    layer_height_mm: f32,
    is_empty_first_layer: bool,
) {
    let (page_number, data_offset) = page_number_and_offset(layer_data_abs_offset);

    let table_size = match layout {
        LayerDefLayout::Encrypted => CTB_ENCRYPTED_LAYER_DEF_SIZE,
        LayerDefLayout::Extended  => CTB_LAYER_DEF_EX_SIZE,
        LayerDefLayout::Basic     => CTB_LAYER_DEF_SIZE,
    };

    let clamp_non_negative = |value: f32| {
        if !value.is_finite() || value <= 0.0 {
            0.0
        } else {
            value
        }
    };

    let is_bottom_wait = is_extended_wait_layer(layer.index, &timing);

    // Use bottom lift distance for bottom layers, normal for others
    let lift_distance = if is_bottom {
        timing.bottom_lift_distance_mm
    } else {
        timing.lift_distance_mm
    };
    let lift_distance2 = if is_bottom {
        timing.bottom_lift_distance2_mm
    } else {
        timing.lift_distance2_mm
    };
    let lift_speed = if is_bottom {
        timing.bottom_lift_speed_mm_min
    } else {
        timing.lift_speed_mm_min
    };
    let lift_speed2 = if is_bottom {
        timing.bottom_lift_speed2_mm_min
    } else {
        timing.lift_speed2_mm_min
    };
    let retract_speed = if is_bottom {
        timing.bottom_retract_speed_mm_min
    } else {
        timing.retract_speed_mm_min
    };
    let retract_speed2 = if is_bottom {
        timing.bottom_retract_speed2_mm_min
    } else {
        timing.retract_speed2_mm_min
    };
    let wait_time_after_cure = if is_bottom_wait {
        timing.bottom_wait_time_after_cure_sec
    } else {
        timing.wait_time_after_cure_sec
    };
    let wait_time_after_lift = if is_bottom_wait {
        timing.bottom_wait_time_after_lift_sec
    } else {
        timing.wait_time_after_lift_sec
    };
    // Per-layer schedule: two-tier bottom/normal, or the wait profile when enabled.
    let wait_time_before_cure = wait_before_cure_sec(layer.index, layer_height_mm, &timing);
    let projector_duty_cycle_pwm = if is_bottom {
        timing.bottom_layer_projector_duty_cycle_pwm
    } else {
        timing.projector_duty_cycle_pwm
    };
    let exposure_sec = layer_exposure_sec(layer.index, is_bottom, &timing);
    let light_off_sec = if is_bottom {
        timing.bottom_light_off_delay_sec
    } else {
        timing.light_off_delay_sec
    };


    // Per-layer CTBv4/v5 semantics follow Chitubox/UVtools LayerDefEx:
    // LiftHeight is total (stage1 + stage2), RetractHeight2 is stage2 retract distance.
    let lift_height_1 = clamp_non_negative(lift_distance);
    let lift_height_2 = clamp_non_negative(lift_distance2);
    let lift_height_total = clamp_non_negative(lift_height_1 + lift_height_2);
    let retract_height_2 = clamp_non_negative(timing.retract_distance2_mm).min(lift_height_total);

    // Whale3 first-layer movement: layer 1 gets a single slow 2.5 mm lift and retract.
    let (lift_height_total, lift_speed, lift_height_2, lift_speed2, retract_speed, retract_height_2, retract_speed2) =
        match first_layer_motion(layer.index, layer_height_mm, &timing) {
            Some(m) => (
                m.lift_mm,
                m.lift_speed_mm_min,
                0.0,
                m.lift_speed_mm_min,
                m.retract_speed_mm_min,
                0.0,
                m.retract_speed_mm_min,
            ),
            None => (
                lift_height_total,
                lift_speed,
                lift_height_2,
                lift_speed2,
                retract_speed,
                retract_height_2,
                retract_speed2,
            ),
        };
    
    
    // The inserted blank layer cures nothing and waits for nothing: its own values
    // replace everything worked out above.
    let (
        exposure_sec,
        light_off_sec,
        wait_time_before_cure,
        wait_time_after_cure,
        wait_time_after_lift,
        projector_duty_cycle_pwm,
        lift_height_total,
        lift_speed,
        lift_height_2,
        lift_speed2,
        retract_speed,
        retract_height_2,
        retract_speed2,
    ) = if is_empty_first_layer {
        let (lift_mm, lift_speed, retract_speed) = empty_first_layer_motion(layer_height_mm, &timing);
        (
            EMPTY_FIRST_LAYER_EXPOSURE_SEC,
            0.0,
            0.0,
            0.0,
            0.0,
            EMPTY_FIRST_LAYER_PWM,
            lift_mm,
            lift_speed,
            0.0,
            lift_speed,
            retract_speed,
            0.0,
            retract_speed,
        )
    } else {
        (
            exposure_sec,
            light_off_sec,
            wait_time_before_cure,
            wait_time_after_cure,
            wait_time_after_lift,
            projector_duty_cycle_pwm,
            lift_height_total,
            lift_speed,
            lift_height_2,
            lift_speed2,
            retract_speed,
            retract_height_2,
            retract_speed2,
        )
    };

    match layout {
        LayerDefLayout::Encrypted => {
            push_u32(out, table_size);
            push_f32(out, position_z_mm);
            push_f32(out, exposure_sec);
            push_f32(out, light_off_sec);
            push_u32(out, data_offset);
            push_u32(out, page_number);
            push_u32(out, layer.encoded.len() as u32);
            push_u32(out, 0); push_u32(out, 0); push_u32(out, 0);
        }
        LayerDefLayout::Basic | LayerDefLayout::Extended => {
            push_f32(out, position_z_mm);
            push_f32(out, exposure_sec);
            push_f32(out, light_off_sec);
            push_u32(out, data_offset);
            push_u32(out, layer.encoded.len() as u32);
            push_u32(out, page_number);
            push_u32(out, table_size);
            push_u32(out, 0); push_u32(out, 0);

            if matches!(layout, LayerDefLayout::Basic) {
                return;
            }
            push_u32(out, table_size + layer.encoded.len() as u32);
        }
    }

    push_f32(out, lift_height_total);
    push_f32(out, clamp_non_negative(lift_speed));
    push_f32(out, lift_height_2);
    push_f32(out, clamp_non_negative(lift_speed2));
    push_f32(out, clamp_non_negative(retract_speed));
    push_f32(out, retract_height_2);
    push_f32(out, clamp_non_negative(retract_speed2));
    push_f32(out, clamp_non_negative(wait_time_after_cure));
    push_f32(out, clamp_non_negative(wait_time_after_lift));
    push_f32(out, clamp_non_negative(wait_time_before_cure));
    push_f32(out, clamp_non_negative(projector_duty_cycle_pwm as f32)); //test as u32/u16

    
    if matches!(layout, LayerDefLayout::Encrypted) { push_u32(out, 0); }
}

/// Estimated print time: per layer, exposure + light-off delay + all three waits +
/// lift/retract travel time, using the same per-layer values the layer table gets.
fn compute_print_time_seconds(
    prepared_count: usize,
    layer_height_mm: f32,
    timing: CtbTimingModel,
) -> u32 {
    let non_negative = |v: f32| if v.is_finite() && v > 0.0 { v } else { 0.0 };
    // Travel time in seconds for a distance (mm) at a speed (mm/min).
    let travel = |distance_mm: f32, speed_mm_min: f32| {
        let (d, s) = (non_negative(distance_mm), non_negative(speed_mm_min));
        if d > 0.0 && s > 0.0 { d / s * 60.0 } else { 0.0 }
    };

    let mut total = 0.0_f64;
    for i in 0..prepared_count {
        let is_bottom = (i as u32) < timing.bottom_layer_count;
        let is_bottom_wait = is_extended_wait_layer(i, &timing);

        let exposure = layer_exposure_sec(i, is_bottom, &timing);
        let light_off = if is_bottom {
            timing.bottom_light_off_delay_sec
        } else {
            timing.light_off_delay_sec
        };
        let (lift1, lift2, lift_speed, lift_speed2, retract_speed, retract_speed2) = if is_bottom {
            (
                timing.bottom_lift_distance_mm,
                timing.bottom_lift_distance2_mm,
                timing.bottom_lift_speed_mm_min,
                timing.bottom_lift_speed2_mm_min,
                timing.bottom_retract_speed_mm_min,
                timing.bottom_retract_speed2_mm_min,
            )
        } else {
            (
                timing.lift_distance_mm,
                timing.lift_distance2_mm,
                timing.lift_speed_mm_min,
                timing.lift_speed2_mm_min,
                timing.retract_speed_mm_min,
                timing.retract_speed2_mm_min,
            )
        };
        let lift_total = non_negative(lift1) + non_negative(lift2);
        let retract2 = non_negative(timing.retract_distance2_mm).min(lift_total);
        let motion = match first_layer_motion(i, layer_height_mm, &timing) {
            Some(m) => travel(m.lift_mm, m.lift_speed_mm_min) + travel(m.retract_mm, m.retract_speed_mm_min),
            None => {
                // The plate comes back down to one layer above where it started.
                let retract_total = (lift_total - non_negative(layer_height_mm)).max(0.0);
                let retract2 = retract2.min(retract_total);
                travel(lift1, lift_speed)
                    + travel(lift2, lift_speed2)
                    + travel(retract_total - retract2, retract_speed)
                    + travel(retract2, retract_speed2)
            }
        };

        let (wait_after_cure, wait_after_lift) = if is_bottom_wait {
            (timing.bottom_wait_time_after_cure_sec, timing.bottom_wait_time_after_lift_sec)
        } else {
            (timing.wait_time_after_cure_sec, timing.wait_time_after_lift_sec)
        };
        let waits = non_negative(wait_before_cure_sec(i, layer_height_mm, &timing))
            + non_negative(wait_after_cure)
            + non_negative(wait_after_lift);

        total += (non_negative(exposure) + non_negative(light_off) + motion + waits + CTB_PER_LAYER_OVERHEAD_SEC) as f64;
    }

    // The inserted blank layer: a flash of exposure and a short hop, no waits.
    if timing.empty_first_layer {
        let (lift_mm, lift_speed, retract_speed) = empty_first_layer_motion(layer_height_mm, &timing);
        total += (EMPTY_FIRST_LAYER_EXPOSURE_SEC
            + travel(lift_mm, lift_speed)
            + travel(lift_mm, retract_speed)
            + CTB_PER_LAYER_OVERHEAD_SEC) as f64;
    }

    total.round().clamp(0.0, u32::MAX as f64) as u32
}

pub(super) fn build_ctb_container_bytes_with_progress(
    job: &SliceJobV3,
    prepared: &[CtbPreparedLayer],
    on_progress: Option<&dyn Fn(u32, u32)>,
) -> Result<Vec<u8>, SlicerV3Error> {
    let timing = parse_timing_model_from_metadata(&job.metadata_json);
    let build = parse_ctb_build_model_from_job(job);
    let caps = ctb_version_caps(build.version);
    let resin = parse_ctb_resin_model_from_job(job, &build.machine_name);

    let previews = build_previews(job)?;

    let mut machine_name_bytes = build.machine_name.as_bytes().to_vec();
    if caps.resin_params && !machine_name_bytes.ends_with(&[0]) {
        machine_name_bytes.push(0);
    }
    let machine_name_size = machine_name_bytes.len() as u32;

    // Optional UVTools-style blank layer in front of the real layers. It is held here
    // and referenced from `effective`, so no layer data is copied.
    let empty_first = if timing.empty_first_layer {
        Some(build_empty_first_layer(
            job.source_width_px,
            job.source_height_px,
            build.layer_xor_key,
        ))
    } else {
        None
    };
    let effective: Vec<&CtbPreparedLayer> = empty_first.iter().chain(prepared.iter()).collect();

    let layer_count = effective.len() as u32;

    let mut offset = CTB_HEADER_SIZE;

    let large_preview_record_offset = offset;
    let large_preview_image_offset = large_preview_record_offset + CTB_PREVIEW_RECORD_SIZE;
    offset = large_preview_image_offset + previews[0].encoded.len() as u32;

    let small_preview_record_offset = offset;
    let small_preview_image_offset = small_preview_record_offset + CTB_PREVIEW_RECORD_SIZE;
    offset = small_preview_image_offset + previews[1].encoded.len() as u32;

    let print_parameters_offset = offset;
    offset += CTB_PRINT_PARAMETERS_SIZE;

    let slicer_offset = offset;
    let slicer_size = CTB_SLICER_INFO_FIXED_SIZE;
    offset += slicer_size;

    let machine_name_offset = offset;
    offset += machine_name_size;

    let disclaimer_bytes = decode_embedded_disclaimer_bytes()?;

    let resin_payload = prepare_resin_payload(&build, &resin, false, caps.resin_params);

    let mut extended_offsets = CtbExtendedOffsets {
        disclaimer_offset: 0,
        disclaimer_length: 0,
        print_parameters_v4_offset: 0,
        resin_parameters_offset: 0,
    };

    if caps.print_params_v4 {
        extended_offsets.disclaimer_offset = offset;
        extended_offsets.disclaimer_length = CTB_DISCLAIMER_SIZE as u32;
        offset += CTB_DISCLAIMER_SIZE as u32;

        extended_offsets.print_parameters_v4_offset = offset;
        offset += CTB_PRINT_PARAMETERS_V4_SIZE;

        if caps.resin_params {
            extended_offsets.resin_parameters_offset = offset;
            offset += resin_payload_len(&resin_payload);
        }
    }

    let layers_definition_offset = offset;

    let mut layer_defs_data = Vec::with_capacity(effective.len() * CTB_LAYER_DEF_EX_SIZE as usize);
    let mut layer_payload_data = Vec::new();

    let print_time_sec = compute_print_time_seconds(prepared.len(), job.layer_height_mm, timing);

    let preview_offsets = CtbPreviewOffsets {
        large_record_offset: large_preview_record_offset,
        small_record_offset: small_preview_record_offset,
    };

    let mut out = Vec::new();
    write_ctb_header(
        &mut out,
        caps.magic,
        build.version,
        job,
        layer_count,
        timing,
        &build,
        preview_offsets.clone(),
        print_parameters_offset,
        layers_definition_offset,
        print_time_sec,
        slicer_offset,
        slicer_size,
    );

    assert_eq!(out.len(), CTB_HEADER_SIZE as usize);

    write_preview_record(
        &mut out,
        previews[0].width,
        previews[0].height,
        large_preview_image_offset,
        previews[0].encoded.len() as u32,
    );
    out.extend_from_slice(&previews[0].encoded);

    write_preview_record(
        &mut out,
        previews[1].width,
        previews[1].height,
        small_preview_image_offset,
        previews[1].encoded.len() as u32,
    );
    out.extend_from_slice(&previews[1].encoded);

    write_ctb_print_parameters(&mut out, timing);
    assert_eq!(
        out.len(),
        print_parameters_offset as usize + CTB_PRINT_PARAMETERS_SIZE as usize
    );

    let pls_flag = if build.per_layer_settings { caps.per_layer_settings_active_flag } else { 0x00 };
    write_ctb_slicer_info_fixed(
        &mut out,
        &build,
        timing,
        machine_name_offset,
        machine_name_size,
        extended_offsets.print_parameters_v4_offset,
        pls_flag,
    );
    assert_eq!(out.len(), slicer_offset as usize + slicer_size as usize);

    out.extend_from_slice(&machine_name_bytes);
    assert_eq!(
        out.len(),
        machine_name_offset as usize + machine_name_size as usize
    );

    if caps.print_params_v4 {
        push_bytes_padded(&mut out, &disclaimer_bytes, CTB_DISCLAIMER_SIZE);
        assert_eq!(
            out.len(),
            extended_offsets.disclaimer_offset as usize + CTB_DISCLAIMER_SIZE
        );

        write_ctb_print_parameters_v4(&mut out, timing, layer_count, extended_offsets);
        assert_eq!(
            out.len(),
            extended_offsets.print_parameters_v4_offset as usize
                + CTB_PRINT_PARAMETERS_V4_SIZE as usize
        );

        if caps.resin_params {
            write_ctb_resin_parameters(
                &mut out,
                extended_offsets.resin_parameters_offset,
                &resin_payload,
                &resin,
            );
            assert_eq!(
                out.len(),
                extended_offsets.resin_parameters_offset as usize
                    + resin_payload_len(&resin_payload) as usize
            );
        }
    }

    assert_eq!(out.len(), layers_definition_offset as usize);

    let layer_def_record_size = CTB_LAYER_DEF_SIZE as usize;
    let layer_defs_total_size = effective.len() * layer_def_record_size;
    let layer_data_start_abs = (layers_definition_offset as u64) + (layer_defs_total_size as u64);

    let total_prepared = effective.len() as u32;
    for (idx, layer) in effective.iter().enumerate() {
        let layer_data_blob_abs = layer_data_start_abs + (layer_payload_data.len() as u64);
        let layer_data_abs = if caps.extended_layer_def {
            layer_data_blob_abs + CTB_LAYER_DEF_EX_SIZE as u64
        } else {
            layer_data_blob_abs
        };

        let position_z_mm = layer_position_z_mm(layer.index, job.layer_height_mm, &timing);

        let mut layer_def_bytes = Vec::new();

        let is_bottom = (layer.index as u32) < timing.bottom_layer_count;
        let is_empty_first_layer = empty_first.is_some() && idx == 0;
        let layout = if caps.extended_layer_def { LayerDefLayout::Extended } else { LayerDefLayout::Basic };
        write_layer_def_ex(
            &mut layer_def_bytes,
            layer,
            position_z_mm,
            timing,
            layer_data_abs,
            layout,
            is_bottom,
            job.layer_height_mm,
            is_empty_first_layer,
        );

        if caps.extended_layer_def {
            layer_defs_data.extend_from_slice(&layer_def_bytes[..CTB_LAYER_DEF_SIZE as usize]);
            layer_payload_data.extend_from_slice(&layer_def_bytes);
        } else {
            layer_defs_data.extend_from_slice(&layer_def_bytes);
        }

        // Prepending a layer moves every real layer one position down the file, and the
        // RLE obfuscation is keyed by position.
        let data_start = layer_payload_data.len();
        layer_payload_data.extend_from_slice(&layer.encoded);
        rekey_layer_rle(
            build.layer_xor_key,
            layer.index as u32,
            idx as u32,
            &mut layer_payload_data[data_start..],
        );

        if let Some(progress) = on_progress {
            progress((idx as u32) + 1, total_prepared.max(1));
        }
    }

    out.extend_from_slice(&layer_defs_data);
    out.extend_from_slice(&layer_payload_data);

    Ok(out)
}

fn write_encrypted_settings(
    out: &mut Vec<u8>,
    build: &CtbBuildModel,
    job: &SliceJobV3,
    timing: CtbTimingModel,
    layer_count: u32,
    print_time_sec: u32,
    large_preview_offset: u32,
    small_preview_offset: u32,
    machine_name_offset: u32,
    machine_name_size: u32,
    disclaimer_offset: u32,
    resin_parameters_address: u32,
    layer_pointers_offset: u32,
    checksum_value: u64,
) {
    // CTB print-parameter lift heights are TOTAL heights (stage1 + stage2).
    let bottom_lift_total_mm = timing.bottom_lift_distance_mm + timing.bottom_lift_distance2_mm;
    let lift_total_mm = timing.lift_distance_mm + timing.lift_distance2_mm;

    push_u64(out, checksum_value);
    push_u32(out, layer_pointers_offset);
    push_f32(out, job.build_width_mm);
    push_f32(out, job.build_depth_mm);
    push_f32(out, build.bed_size_z_mm.max(0.0));
    push_u32(out, build.created_date_unix);
    push_u32(out, build.modified_date_unix);
    // The inserted blank layer shares the real first layer's Z, so it adds no height,
    // but it does count as one more bottom layer so the real bottom layers keep theirs.
    push_f32(out, job.layer_height_mm * layer_count.saturating_sub(empty_layer_count(&timing)) as f32);
    push_f32(out, job.layer_height_mm);
    push_f32(out, timing.normal_exposure_sec);
    push_f32(out, timing.bottom_exposure_sec);
    push_f32(out, timing.light_off_delay_sec);
    push_u32(out, timing.bottom_layer_count + empty_layer_count(&timing));
    push_u32(out, job.source_width_px);
    push_u32(out, job.source_height_px);
    push_u32(out, layer_count);
    push_u32(out, large_preview_offset);
    push_u32(out, small_preview_offset);
    push_u32(out, print_time_sec);
    push_u32(out, build.projector_type);
    push_f32(out, bottom_lift_total_mm.max(0.0));
    push_f32(out, timing.bottom_lift_speed_mm_min);
    push_f32(out, lift_total_mm.max(0.0));
    push_f32(out, timing.lift_speed_mm_min);
    push_f32(out, timing.retract_speed_mm_min);
    push_f32(out, timing.retract_distance_mm);
    push_f32(out, timing.retract_distance2_mm);
    push_f32(out, timing.retract_speed2_mm_min);
    push_f32(out, timing.bottom_light_off_delay_sec);
    push_u32(out, 1);
    push_u16(out, timing.projector_duty_cycle_pwm);
    push_u16(out, timing.bottom_layer_projector_duty_cycle_pwm);
    push_u32(out, build.layer_xor_key);
    // Slots: BottomLiftHeight2, BottomLiftSpeed2, LiftHeight2, LiftSpeed2, RetractHeight2, RetractSpeed2.
    push_f32(out, timing.bottom_lift_distance2_mm);
    push_f32(out, timing.bottom_lift_speed2_mm_min);
    push_f32(out, timing.lift_distance2_mm);
    push_f32(out, timing.lift_speed2_mm_min);
    push_f32(out, timing.retract_distance2_mm);
    push_f32(out, timing.retract_speed2_mm_min);
    push_f32(out, timing.wait_time_after_lift_sec);
    push_u32(out, machine_name_offset);
    push_u32(out, machine_name_size);
    push_u8(out, if build.anti_alias_level > 1 { 0x0f } else { 0x07 });
    push_u16(out, 0);
    push_u8(out, if build.per_layer_settings { 0x40 } else { 0x00 });
    push_u32(out, build.modified_date_unix / 60);
    push_u32(out, build.anti_alias_level);
    push_f32(out, timing.wait_time_before_cure_sec);
    push_f32(out, timing.wait_time_after_lift_sec);
    push_u32(out, timing.transition_layer_count);
    push_f32(out, timing.bottom_retract_speed_mm_min);
    push_f32(out, timing.bottom_retract_speed2_mm_min);
    push_u32(out, 0);
    push_f32(out, timing.retract_distance_mm.max(0.0));
    push_u32(out, 0);
    push_f32(out, timing.retract_distance2_mm.max(0.0));
    push_f32(out, timing.wait_time_before_cure_sec);
    push_f32(out, timing.wait_time_after_lift_sec);
    push_f32(out, timing.wait_time_after_cure_sec);
    push_f32(out, timing.bottom_retract_height2_mm);
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 4);
    push_u32(out, layer_count.saturating_sub(1));
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, 0);
    push_u32(out, disclaimer_offset);
    push_u32(out, CTB_DISCLAIMER_SIZE as u32);
    push_u32(out, 0);
    push_u32(out, resin_parameters_address);
    push_u32(out, 0);
    push_u32(out, 0);
}

fn write_encrypted_header(
    out: &mut Vec<u8>,
    version: u32,
    signature_len: u32,
    signature_offset: u32,
) {
    push_u32(out, CTB_MAGIC_V5_ENCRYPTED);
    push_u32(out, CTB_ENCRYPTED_SETTINGS_SIZE);
    push_u32(out, CTB_ENCRYPTED_SETTINGS_OFFSET);
    push_u32(out, 0);
    push_u32(out, version);
    push_u32(out, signature_len);
    push_u32(out, signature_offset);
    push_u32(out, 0);
    push_u16(out, 1);
    push_u16(out, 1);
    push_u32(out, 0);
    push_u32(out, 42);
    push_u32(out, 0);
}

pub(super) fn build_ctb_encrypted_container_bytes_with_progress(
    job: &SliceJobV3,
    prepared: &[CtbPreparedLayer],
    on_progress: Option<&dyn Fn(u32, u32)>,
) -> Result<Vec<u8>, SlicerV3Error> {
    let timing = parse_timing_model_from_metadata(&job.metadata_json);
    let mut build = parse_ctb_build_model_from_job(job);
    // Encrypted format supports V3–V5; clamp to that range, defaulting to V5.
    if build.version < 3 || build.version > 5 {
        build.version = 5;
    }
    let caps = ctb_version_caps(build.version);
    let resin = parse_ctb_resin_model_from_job(job, &build.machine_name);
    let previews = build_previews(job)?;
    let disclaimer_bytes = decode_embedded_disclaimer_bytes()?;
    let (key, iv) = ctb_default_key_iv();

    let empty_first = if timing.empty_first_layer {
        Some(build_empty_first_layer(
            job.source_width_px,
            job.source_height_px,
            build.layer_xor_key,
        ))
    } else {
        None
    };
    let effective: Vec<&CtbPreparedLayer> = empty_first.iter().chain(prepared.iter()).collect();

    let layer_count = effective.len() as u32;
    let print_time_sec = compute_print_time_seconds(prepared.len(), job.layer_height_mm, timing);
    let machine_name_bytes = build.machine_name.as_bytes().to_vec();
    let machine_name_size = machine_name_bytes.len() as u32;

    let mut out = vec![0u8; (CTB_ENCRYPTED_HEADER_SIZE + CTB_ENCRYPTED_SETTINGS_SIZE) as usize];

    let large_preview_offset = out.len() as u32;
    write_preview_record(&mut out, previews[0].width, previews[0].height, large_preview_offset + CTB_PREVIEW_RECORD_SIZE, previews[0].encoded.len() as u32);
    out.extend_from_slice(&previews[0].encoded);

    let small_preview_offset = out.len() as u32;
    write_preview_record(&mut out, previews[1].width, previews[1].height, small_preview_offset + CTB_PREVIEW_RECORD_SIZE, previews[1].encoded.len() as u32);
    out.extend_from_slice(&previews[1].encoded);

    let machine_name_offset = out.len() as u32;
    out.extend_from_slice(&machine_name_bytes);

    let disclaimer_offset = out.len() as u32;
    let mut disclaimer_padded = Vec::new();
    if disclaimer_bytes.len() >= CTB_DISCLAIMER_SIZE {
        disclaimer_padded.extend_from_slice(&disclaimer_bytes[..CTB_DISCLAIMER_SIZE]);
    } else {
        disclaimer_padded.extend_from_slice(&disclaimer_bytes);
        disclaimer_padded.resize(CTB_DISCLAIMER_SIZE, 0);
    }
    out.extend_from_slice(&disclaimer_padded);

    // V5enc only: ResinParameters block (resin color, type, name, density).
    let resin_parameters_address = if caps.resin_params {
        let mut enc_resin = resin.clone();
        if enc_resin.resin_type.is_empty() { enc_resin.resin_type = "Standard".to_string(); }
        if enc_resin.resin_name.is_empty() { enc_resin.resin_name = "DragonFruit Resin".to_string(); }
        let payload = prepare_resin_payload(&build, &enc_resin, false, false);
        let addr = out.len() as u32;
        write_ctb_resin_parameters(&mut out, addr, &payload, &enc_resin);
        addr
    } else {
        0u32
    };

    let layer_pointers_offset = out.len() as u32;
    let layer_pointer_table_size = layer_count as usize * 16;
    let layer_pointers_table_start = out.len();
    out.resize(out.len() + layer_pointer_table_size, 0);

    let mut layer_pointer_entries: Vec<(u32, u32)> = Vec::with_capacity(layer_count as usize);
    let total_layers = effective.len() as u32;
    for (idx, layer) in effective.iter().enumerate() {
        let layer_def_abs = out.len() as u64;
        let layer_data_abs = layer_def_abs + CTB_ENCRYPTED_LAYER_DEF_SIZE as u64;
        let (layer_page, layer_offset) = page_number_and_offset(layer_def_abs);
        layer_pointer_entries.push((layer_offset, layer_page));

        let is_bottom = (layer.index as u32) < timing.bottom_layer_count;
        let is_empty_first_layer = empty_first.is_some() && idx == 0;
        write_layer_def_ex(&mut out, layer, layer_position_z_mm(layer.index, job.layer_height_mm, &timing), timing, layer_data_abs, LayerDefLayout::Encrypted, is_bottom, job.layer_height_mm, is_empty_first_layer);
        let data_start = out.len();
        out.extend_from_slice(&layer.encoded);
        rekey_layer_rle(build.layer_xor_key, layer.index as u32, idx as u32, &mut out[data_start..]);

        if let Some(progress) = on_progress {
            progress((idx as u32) + 1, total_layers.max(1));
        }
    }

    // V5enc only: 8-byte post-layer footer required by firmware.
    if caps.resin_params {
        push_u32(&mut out, 1_109_414_650);
        push_u32(&mut out, 0);
    }

    let checksum_value: u64 = 0xCAFE_BABE;
    let checksum_hash = Sha256::digest(checksum_value.to_le_bytes());
    let mut signature = checksum_hash.to_vec();
    ctb_encrypt_in_place_no_padding(&mut signature, &key, &iv)?;
    let signature_offset = out.len() as u32;
    out.extend_from_slice(&signature);
    // V4enc+: 4-byte trailing marker.
    if caps.print_params_v4 {
        push_u32(&mut out, 1_833_054_899);
    }

    for (i, (layer_offset, layer_page)) in layer_pointer_entries.iter().enumerate() {
        let base = layer_pointers_table_start + i * 16;
        out[base..base + 4].copy_from_slice(&layer_offset.to_le_bytes());
        out[base + 4..base + 8].copy_from_slice(&layer_page.to_le_bytes());
        out[base + 8..base + 12].copy_from_slice(&CTB_ENCRYPTED_LAYER_DEF_SIZE.to_le_bytes());
        out[base + 12..base + 16].copy_from_slice(&0u32.to_le_bytes());
    }

    let mut settings = Vec::with_capacity(CTB_ENCRYPTED_SETTINGS_SIZE as usize);
    write_encrypted_settings(&mut settings, &build, job, timing, layer_count, print_time_sec, large_preview_offset, small_preview_offset, machine_name_offset, machine_name_size, disclaimer_offset, resin_parameters_address, layer_pointers_offset, checksum_value);
    if settings.len() != CTB_ENCRYPTED_SETTINGS_SIZE as usize {
        return Err(SlicerV3Error::UnsupportedOutput(format!(
            "internal encrypted CTB settings size mismatch: expected {}, got {}",
            CTB_ENCRYPTED_SETTINGS_SIZE, settings.len()
        )));
    }
    ctb_encrypt_in_place_no_padding(&mut settings, &key, &iv)?;
    let settings_start = CTB_ENCRYPTED_SETTINGS_OFFSET as usize;
    out[settings_start..settings_start + CTB_ENCRYPTED_SETTINGS_SIZE as usize].copy_from_slice(&settings);

    let mut header = Vec::with_capacity(CTB_ENCRYPTED_HEADER_SIZE as usize);
    write_encrypted_header(&mut header, build.version, signature.len() as u32, signature_offset);
    if header.len() != CTB_ENCRYPTED_HEADER_SIZE as usize {
        return Err(SlicerV3Error::UnsupportedOutput(format!(
            "internal encrypted CTB header size mismatch: expected {}, got {}",
            CTB_ENCRYPTED_HEADER_SIZE, header.len()
        )));
    }
    out[..CTB_ENCRYPTED_HEADER_SIZE as usize].copy_from_slice(&header);

    Ok(out)
}