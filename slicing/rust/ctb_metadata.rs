use crate::engine::SlicerV3Error;
use crate::types::SliceJobV3;
use base64::Engine;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use super::ctb_types::{
    CtbBuildModel, CtbResinModel, CtbTimingModel, CTB_DISCLAIMER_B64, DEFAULT_BINARY_THRESHOLD,
    DEFAULT_CTB_VERSION, DEFAULT_MACHINE_NAME, DEFAULT_RESIN_DENSITY, DEFAULT_RESIN_NAME,
    DEFAULT_RESIN_TYPE,
};

fn parse_json(metadata_json: &str) -> Option<Value> {
    serde_json::from_str::<Value>(metadata_json).ok()
}

pub(super) fn parse_ctb_format_version_hint(value: Option<&str>) -> Option<(u32, bool)> {
    let raw = value?.trim();
    if raw.is_empty() {
        return None;
    }

    let normalized: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect();

    match normalized.as_str() {
        "2" | "v2" => Some((2, false)),
        "3" | "v3" | "v2v3" | "ctbv2v3" => Some((3, false)),
        "4" | "v4" | "v4v5" | "ctbv4v5" => Some((4, false)),
        "5" | "v5" => Some((5, false)),
        "v3enc" | "v3encrypted" | "v3aes" | "ctbv3enc" => Some((3, true)),
        "v4enc" | "v4encrypted" | "v4aes" | "ctbv4enc" => Some((4, true)),
        "v5enc" | "v5encrypted" | "v5encryption" | "v5aes" | "ctbv5enc" => Some((5, true)),
        _ => None,
    }
}

pub(super) fn parse_ctb_format_version_hint_from_job(job: &SliceJobV3) -> Option<(u32, bool)> {
    parse_ctb_format_version_hint(job.format_version.as_deref())
}

pub(super) fn parse_threshold_from_metadata(metadata_json: &str) -> u8 {
    let Some(meta) = parse_json(metadata_json) else {
        return DEFAULT_BINARY_THRESHOLD;
    };

    let direct = meta
        .get("ctb")
        .and_then(|v| v.get("binaryThreshold"))
        .and_then(Value::as_u64);

    let nested = meta
        .get("export")
        .and_then(|v| v.get("ctb"))
        .and_then(|v| v.get("binaryThreshold"))
        .and_then(Value::as_u64);

    direct
        .or(nested)
        .map(|v| v.min(255) as u8)
        .unwrap_or(DEFAULT_BINARY_THRESHOLD)
}

pub(super) fn parse_timing_model_from_metadata(metadata_json: &str) -> CtbTimingModel {
    let Some(meta) = parse_json(metadata_json) else {
        return CtbTimingModel {
            normal_exposure_sec: 0.0,
            bottom_exposure_sec: 0.0,
            light_off_delay_sec: 0.0,
            bottom_light_off_delay_sec: 0.0,
            bottom_layer_count: 0,
            lift_distance_mm: 0.0,
            lift_distance2_mm: 0.0,
            lift_speed_mm_min: 0.0,
            lift_speed2_mm_min: 0.0,
            retract_distance_mm: 0.0,
            retract_distance2_mm: 0.0,
            retract_speed_mm_min: 0.0,
            retract_speed2_mm_min: 0.0,
            bottom_lift_distance_mm: 0.0,
            bottom_lift_distance2_mm: 0.0,
            bottom_lift_speed_mm_min: 0.0,
            bottom_lift_speed2_mm_min: 0.0,
            bottom_retract_speed_mm_min: 0.0,
            bottom_retract_speed2_mm_min: 0.0,
            bottom_retract_height2_mm: 0.0,
            transition_layer_count: 0,
            wait_time_before_cure_sec: 0.0,
            wait_time_after_cure_sec: 0.0,
            wait_time_after_lift_sec: 0.0,
            wait_time_bottom_layer_count: 0,
            bottom_wait_time_before_cure_sec: 0.0,
            bottom_wait_time_after_cure_sec: 0.0,
            bottom_wait_time_after_lift_sec: 0.0,
            projector_duty_cycle_pwm: 0,
            bottom_layer_projector_duty_cycle_pwm: 0,
        };
    };

    let material = meta.get("material").and_then(Value::as_object);
    let ctb = meta.get("ctb").and_then(Value::as_object);
    let export_ctb = meta
        .get("export")
        .and_then(|v| v.get("ctb"))
        .and_then(Value::as_object);
    let ctb_timing = ctb
        .and_then(|obj| obj.get("timing"))
        .and_then(Value::as_object);
    let export_ctb_timing = export_ctb
        .and_then(|obj| obj.get("timing"))
        .and_then(Value::as_object);

    let settings_mode = ctb
        .and_then(|m| m.get("settingsMode"))
        .and_then(Value::as_str)
        .or_else(|| ctb.and_then(|m| m.get("mode")).and_then(Value::as_str))
        .or_else(|| {
            export_ctb
                .and_then(|m| m.get("settingsMode"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            export_ctb
                .and_then(|m| m.get("mode"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            meta.get("printer")
                .and_then(Value::as_object)
                .and_then(|m| m.get("settingsMode"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            meta.get("printer")
                .and_then(Value::as_object)
                .and_then(|m| m.get("mode"))
                .and_then(Value::as_str)
        })
        .map(|v| v.trim().to_ascii_lowercase());
    let is_simple_mode = matches!(settings_mode.as_deref(), Some("simple"));

    // Beta one step for S4U tilting + bottom wait times 
    let is_beta_simple_mode = matches!(settings_mode.as_deref(), Some("betaonestep"));
    let is_tilting_mode = matches!(settings_mode.as_deref(), Some("tilting"));


    let read_f32 = |key: &str| {
        ctb.and_then(|m| m.get(key))
            .and_then(Value::as_f64)
            .or_else(|| ctb_timing.and_then(|m| m.get(key)).and_then(Value::as_f64))
            .or_else(|| export_ctb.and_then(|m| m.get(key)).and_then(Value::as_f64))
            .or_else(|| {
                export_ctb_timing
                    .and_then(|m| m.get(key))
                    .and_then(Value::as_f64)
            })
            .or_else(|| material.and_then(|m| m.get(key)).and_then(Value::as_f64))
            .unwrap_or(0.0) as f32
    };
    let read_u32 = |key: &str| {
        ctb.and_then(|m| m.get(key))
            .and_then(Value::as_u64)
            .or_else(|| ctb_timing.and_then(|m| m.get(key)).and_then(Value::as_u64))
            .or_else(|| export_ctb.and_then(|m| m.get(key)).and_then(Value::as_u64))
            .or_else(|| {
                export_ctb_timing
                    .and_then(|m| m.get(key))
                    .and_then(Value::as_u64)
            })
            .or_else(|| material.and_then(|m| m.get(key)).and_then(Value::as_u64))
            .unwrap_or(0) as u32
    };

    let mut timing = CtbTimingModel {
        normal_exposure_sec: read_f32("normalExposureSec"),
        bottom_exposure_sec: read_f32("bottomExposureSec"),
        light_off_delay_sec: read_f32("lightOffDelaySec"),
        bottom_light_off_delay_sec: read_f32("bottomLightOffDelaySec"),
        bottom_layer_count: read_u32("bottomLayerCount"),
        lift_distance_mm: read_f32("liftDistanceMm"),
        lift_distance2_mm: read_f32("liftDistance2Mm"),
        lift_speed_mm_min: read_f32("liftSpeedMmMin"),
        lift_speed2_mm_min: read_f32("liftSpeed2MmMin"),
        retract_distance_mm: read_f32("retractDistanceMm"),
        retract_distance2_mm: read_f32("retractDistance2Mm"),
        retract_speed_mm_min: read_f32("retractSpeedMmMin"),
        retract_speed2_mm_min: read_f32("retractSpeed2MmMin"),
        bottom_lift_distance_mm: read_f32("bottomLiftDistanceMm"),
        bottom_lift_distance2_mm: read_f32("bottomLiftDistance2Mm"),
        bottom_lift_speed_mm_min: read_f32("bottomLiftSpeedMmMin"),
        bottom_lift_speed2_mm_min: read_f32("bottomLiftSpeed2MmMin"),
        bottom_retract_speed_mm_min: read_f32("bottomRetractSpeedMmMin"),
        bottom_retract_speed2_mm_min: read_f32("bottomRetractSpeed2MmMin"),
        bottom_retract_height2_mm: read_f32("bottomRetractHeight2Mm"),
        transition_layer_count: read_u32("transitionLayerCount"),
        wait_time_before_cure_sec: read_f32("waitTimeBeforeCureSec"),
        wait_time_after_cure_sec: read_f32("waitTimeAfterCureSec"),
        wait_time_after_lift_sec: read_f32("waitTimeAfterLiftSec"),
        wait_time_bottom_layer_count: read_u32("waitTimeBottomLayerCount"),
        bottom_wait_time_before_cure_sec: read_f32("bottomWaitTimeBeforeCureSec"),
        bottom_wait_time_after_cure_sec: read_f32("bottomWaitTimeAfterCureSec"),
        bottom_wait_time_after_lift_sec: read_f32("bottomWaitTimeAfterLiftSec"),
        projector_duty_cycle_pwm: ((read_u32("projectorPwmPercent") as f32) * 2.55) as u16,
        bottom_layer_projector_duty_cycle_pwm : ((read_u32("bottomProjectorPwmPercent") as f32) * 2.55) as u16,
    };

    let sanitize_non_negative = |value: f32| {
        if !value.is_finite() || value <= 0.0 {
            0.0
        } else {
            value
        }
    };

    timing.normal_exposure_sec = sanitize_non_negative(timing.normal_exposure_sec);
    timing.bottom_exposure_sec = sanitize_non_negative(timing.bottom_exposure_sec);
    timing.light_off_delay_sec = sanitize_non_negative(timing.light_off_delay_sec);
    timing.bottom_light_off_delay_sec = sanitize_non_negative(timing.bottom_light_off_delay_sec);
    timing.lift_distance_mm = sanitize_non_negative(timing.lift_distance_mm);
    timing.lift_distance2_mm = sanitize_non_negative(timing.lift_distance2_mm);
    timing.lift_speed_mm_min = sanitize_non_negative(timing.lift_speed_mm_min);
    timing.lift_speed2_mm_min = sanitize_non_negative(timing.lift_speed2_mm_min);
    timing.retract_distance_mm = sanitize_non_negative(timing.retract_distance_mm);
    timing.retract_distance2_mm = sanitize_non_negative(timing.retract_distance2_mm);
    timing.retract_speed_mm_min = sanitize_non_negative(timing.retract_speed_mm_min);
    timing.retract_speed2_mm_min = sanitize_non_negative(timing.retract_speed2_mm_min);
    timing.bottom_lift_distance_mm = sanitize_non_negative(timing.bottom_lift_distance_mm);
    timing.bottom_lift_distance2_mm = sanitize_non_negative(timing.bottom_lift_distance2_mm);
    timing.bottom_lift_speed_mm_min = sanitize_non_negative(timing.bottom_lift_speed_mm_min);
    timing.bottom_lift_speed2_mm_min = sanitize_non_negative(timing.bottom_lift_speed2_mm_min);
    timing.bottom_retract_speed_mm_min = sanitize_non_negative(timing.bottom_retract_speed_mm_min);
    timing.bottom_retract_speed2_mm_min =
        sanitize_non_negative(timing.bottom_retract_speed2_mm_min);
    timing.bottom_retract_height2_mm = sanitize_non_negative(timing.bottom_retract_height2_mm);
    timing.wait_time_before_cure_sec = sanitize_non_negative(timing.wait_time_before_cure_sec);
    timing.wait_time_after_cure_sec = sanitize_non_negative(timing.wait_time_after_cure_sec);
    timing.wait_time_after_lift_sec = sanitize_non_negative(timing.wait_time_after_lift_sec);
    timing.bottom_wait_time_before_cure_sec = sanitize_non_negative(timing.bottom_wait_time_before_cure_sec);
    timing.bottom_wait_time_after_cure_sec = sanitize_non_negative(timing.bottom_wait_time_after_cure_sec);
    timing.bottom_wait_time_after_lift_sec = sanitize_non_negative(timing.bottom_wait_time_after_lift_sec);

    if timing.projector_duty_cycle_pwm <= 0 {
        timing.projector_duty_cycle_pwm = 255;
    }
    if timing.bottom_layer_projector_duty_cycle_pwm <= 0 {
        timing.bottom_layer_projector_duty_cycle_pwm = 255;
    }

    if timing.lift_distance2_mm <= 0.0 {
        timing.lift_distance2_mm = timing.lift_distance_mm;
    }
    if timing.lift_speed2_mm_min <= 0.0 {
        timing.lift_speed2_mm_min = timing.lift_speed_mm_min;
    }
    if timing.retract_distance_mm <= 0.0 {
        timing.retract_distance_mm = timing.lift_distance_mm;
    }
    if timing.retract_distance2_mm <= 0.0 {
        timing.retract_distance2_mm = if timing.bottom_retract_height2_mm > 0.0 {
            timing.bottom_retract_height2_mm
        } else {
            timing.retract_distance_mm
        };
    }
    if timing.retract_speed2_mm_min <= 0.0 {
        timing.retract_speed2_mm_min = if timing.bottom_retract_speed2_mm_min > 0.0 {
            timing.bottom_retract_speed2_mm_min
        } else if timing.bottom_retract_speed_mm_min > 0.0 {
            timing.bottom_retract_speed_mm_min
        } else {
            timing.retract_speed_mm_min
        };
    }

    if timing.bottom_retract_speed_mm_min <= 0.0 {
        timing.bottom_retract_speed_mm_min = timing.retract_speed_mm_min;
    }
    if timing.bottom_retract_speed2_mm_min <= 0.0 {
        timing.bottom_retract_speed2_mm_min = timing.retract_speed2_mm_min;
    }
    if timing.bottom_retract_height2_mm <= 0.0 {
        timing.bottom_retract_height2_mm = timing.retract_distance2_mm;
    }

    if timing.wait_time_bottom_layer_count < timing.bottom_layer_count {
        timing.wait_time_bottom_layer_count = timing.bottom_layer_count;
    }

    if is_simple_mode {
        timing.lift_distance2_mm = 0.0;
        timing.lift_speed2_mm_min = 0.0;
        timing.retract_distance2_mm = 0.0;
        timing.retract_speed2_mm_min = 0.0;
        timing.bottom_lift_distance2_mm = 0.0;
        timing.bottom_lift_speed2_mm_min = 0.0;
        timing.bottom_retract_speed2_mm_min = 0.0;
        timing.bottom_retract_height2_mm = 0.0;
        
        timing.bottom_wait_time_after_cure_sec = 0.0;
        timing.bottom_wait_time_after_lift_sec = 0.0;
        timing.bottom_wait_time_before_cure_sec = 0.0;
    }

    if is_beta_simple_mode {
        timing.lift_distance2_mm = 0.0;
        timing.lift_speed2_mm_min = 0.0;
        timing.retract_distance2_mm = 0.0;
        timing.retract_speed2_mm_min = 0.0;
        timing.bottom_lift_distance2_mm = 0.0;
        timing.bottom_lift_speed2_mm_min = 0.0;
        timing.bottom_retract_speed2_mm_min = 0.0;
        timing.bottom_retract_height2_mm = 0.0;
    }

    if is_tilting_mode {
        timing.lift_speed_mm_min, timing.bottom_lift_speed_mm_min = 60.0;
        timing.retract_speed_mm_min, timing.bottom_retract_speed_mm_min = 150.0;
        timing.lift_distance_mm = read_f32("layerHeightMm");
        timing.bottom_lift_distance_mm = read_f32("layerHeightMm");
    }

    timing
}

fn parse_u32_meta(meta: &Value, path0: &str, path1: &str) -> Option<u32> {
    meta.get(path0)
        .and_then(|v| v.get(path1))
        .and_then(Value::as_u64)
        .map(|v| v as u32)
}

fn parse_str_meta(meta: &Value, path0: &str, path1: &str) -> Option<String> {
    meta.get(path0)
        .and_then(|v| v.get(path1))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn parse_bool_meta(meta: &Value, path0: &str, path1: &str) -> Option<bool> {
    meta.get(path0)
        .and_then(|v| v.get(path1))
        .and_then(Value::as_bool)
}

fn parse_f32_meta(meta: &Value, path0: &str, path1: &str) -> Option<f32> {
    meta.get(path0)
        .and_then(|v| v.get(path1))
        .and_then(Value::as_f64)
        .map(|v| v as f32)
}

fn parse_timestamp_meta(meta: &Value, path0: &str, path1: &str) -> Option<u32> {
    let node = meta.get(path0).and_then(|v| v.get(path1))?;
    if let Some(v) = node.as_u64() {
        return Some(v.min(u32::MAX as u64) as u32);
    }
    if let Some(v) = node.as_i64() {
        return Some(v.max(0).min(u32::MAX as i64) as u32);
    }
    if let Some(s) = node.as_str() {
        if let Ok(parsed) = s.trim().parse::<u64>() {
            return Some(parsed.min(u32::MAX as u64) as u32);
        }
    }
    None
}

fn unix_now_u32() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs().min(u32::MAX as u64) as u32)
        .unwrap_or(0)
}

fn default_layer_xor_key() -> u32 {
    // Must be deterministic because build metadata is parsed in multiple stages.
    // If this value changes between stages, layer RLE bytes are encrypted with one
    // key but header advertises another, causing corrupted layer decode in UVTools.
    0xEFBE_ADDE
}

pub(super) fn parse_ctb_build_model_from_job(job: &SliceJobV3) -> CtbBuildModel {
    let mut version = DEFAULT_CTB_VERSION;
    let mut machine_name = DEFAULT_MACHINE_NAME.to_string();
    let mut bed_size_z_mm = (job.layer_height_mm.max(0.0) * (job.total_layers as f32)).max(0.0);
    let mut created_date_unix = unix_now_u32();
    let mut modified_date_unix = created_date_unix;
    let mut anti_alias_level = 1_u32;
    let mut layer_xor_key = 0_u32;
    let mut projector_type = if job.mirror_x || job.mirror_y { 1 } else { 0 };
    let mut per_layer_settings = false;
    let format_hint = parse_ctb_format_version_hint_from_job(job);

    if let Some(meta) = parse_json(&job.metadata_json) {
        let version_direct = parse_u32_meta(&meta, "ctb", "version");
        let version_nested = meta
            .get("export")
            .and_then(|v| v.get("ctb"))
            .and_then(|v| v.get("version"))
            .and_then(Value::as_u64)
            .map(|v| v as u32);

        version = version_direct
            .or(version_nested)
            .unwrap_or(DEFAULT_CTB_VERSION)
            .clamp(2, 5);

        let machine_direct = parse_str_meta(&meta, "ctb", "machineName");
        let machine_nested = meta
            .get("export")
            .and_then(|v| v.get("ctb"))
            .and_then(|v| v.get("machineName"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        machine_name = machine_direct
            .or(machine_nested)
            .unwrap_or_else(|| DEFAULT_MACHINE_NAME.to_string());

        if let Some(v) = parse_str_meta(&meta, "ctb", "MachineName")
            .or_else(|| {
                meta.get("export")
                    .and_then(|v| v.get("ctb"))
                    .and_then(|v| v.get("MachineName"))
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .or_else(|| parse_str_meta(&meta, "printer", "machineName"))
            .or_else(|| parse_str_meta(&meta, "printer", "MachineName"))
            .or_else(|| parse_str_meta(&meta, "printer", "name"))
        {
            if !v.trim().is_empty() {
                machine_name = v;
            }
        }

        if let Some(v) = parse_f32_meta(&meta, "ctb", "bedSizeZ")
            .or_else(|| parse_f32_meta(&meta, "ctb", "BedSizeZ"))
            .or_else(|| {
                meta.get("export")
                    .and_then(|x| x.get("ctb"))
                    .and_then(|x| x.get("bedSizeZ"))
                    .and_then(Value::as_f64)
                    .map(|x| x as f32)
            })
            .or_else(|| {
                meta.get("export")
                    .and_then(|x| x.get("ctb"))
                    .and_then(|x| x.get("BedSizeZ"))
                    .and_then(Value::as_f64)
                    .map(|x| x as f32)
            })
            .or_else(|| {
                meta.get("printer")
                    .and_then(|x| x.get("buildVolumeMm"))
                    .and_then(|x| x.get("height"))
                    .and_then(Value::as_f64)
                    .map(|x| x as f32)
            })
            .or_else(|| parse_f32_meta(&meta, "printer", "bedSizeZ"))
            .or_else(|| parse_f32_meta(&meta, "printer", "BedSizeZ"))
        {
            if v.is_finite() && v > 0.0 {
                bed_size_z_mm = v;
            }
        }

        if let Some(v) = parse_timestamp_meta(&meta, "ctb", "createdDate")
            .or_else(|| parse_timestamp_meta(&meta, "ctb", "CreatedDate"))
            .or_else(|| {
                parse_timestamp_meta(&meta, "export", "createdDate")
                    .or_else(|| parse_timestamp_meta(&meta, "export", "CreatedDate"))
            })
        {
            created_date_unix = v;
        }

        if let Some(v) = parse_timestamp_meta(&meta, "ctb", "modifiedDate")
            .or_else(|| parse_timestamp_meta(&meta, "ctb", "ModifiedDate"))
            .or_else(|| {
                meta.get("export")
                    .and_then(|x| x.get("ctb"))
                    .and_then(|x| x.get("modifiedDate"))
                    .and_then(Value::as_u64)
                    .map(|x| x.min(u32::MAX as u64) as u32)
            })
            .or_else(|| {
                meta.get("export")
                    .and_then(|x| x.get("ctb"))
                    .and_then(|x| x.get("ModifiedDate"))
                    .and_then(Value::as_u64)
                    .map(|x| x.min(u32::MAX as u64) as u32)
            })
            .or_else(|| parse_timestamp_meta(&meta, "metadata", "modifiedDate"))
            .or_else(|| parse_timestamp_meta(&meta, "metadata", "ModifiedDate"))
        {
            modified_date_unix = v;
        }

        let aa_direct = parse_u32_meta(&meta, "ctb", "antiAliasLevel");
        let aa_nested = meta
            .get("export")
            .and_then(|v| v.get("ctb"))
            .and_then(|v| v.get("antiAliasLevel"))
            .and_then(Value::as_u64)
            .map(|v| v as u32);
        anti_alias_level = aa_direct.or(aa_nested).unwrap_or(1).clamp(1, 16);

        let key_direct = parse_u32_meta(&meta, "ctb", "layerXorKey");
        let key_nested = meta
            .get("export")
            .and_then(|v| v.get("ctb"))
            .and_then(|v| v.get("layerXorKey"))
            .and_then(Value::as_u64)
            .map(|v| v as u32);
        layer_xor_key = key_direct.or(key_nested).unwrap_or(0);

        let proj_direct = parse_u32_meta(&meta, "ctb", "projectorType");
        let proj_nested = meta
            .get("export")
            .and_then(|v| v.get("ctb"))
            .and_then(|v| v.get("projectorType"))
            .and_then(Value::as_u64)
            .map(|v| v as u32);
        projector_type = proj_direct.or(proj_nested).unwrap_or(projector_type);

        let settings_mode = meta
            .get("ctb")
            .and_then(Value::as_object)
            .and_then(|m| m.get("settingsMode").or_else(|| m.get("mode")))
            .and_then(Value::as_str)
            .or_else(|| {
                meta.get("export")
                    .and_then(|v| v.get("ctb"))
                    .and_then(Value::as_object)
                    .and_then(|m| m.get("settingsMode").or_else(|| m.get("mode")))
                    .and_then(Value::as_str)
            })
            .or_else(|| {
                meta.get("printer")
                    .and_then(Value::as_object)
                    .and_then(|m| m.get("settingsMode").or_else(|| m.get("mode")))
                    .and_then(Value::as_str)
            })
            .map(|v| v.trim().to_ascii_lowercase());

        let ctb_root = meta.get("ctb").and_then(Value::as_object);
        let export_ctb_root = meta
            .get("export")
            .and_then(|v| v.get("ctb"))
            .and_then(Value::as_object);

        let read_positive_f32 =
            |root: Option<&serde_json::Map<String, Value>>, key: &str| -> bool {
                root.and_then(|m| m.get(key))
                    .and_then(Value::as_f64)
                    .map(|v| v > 0.0)
                    .unwrap_or(false)
            };

        let has_explicit_two_stage_motion = [
            "liftDistance2Mm",
            "liftSpeed2MmMin",
            "bottomLiftDistanceMm",
            "bottomLiftDistance2Mm",
            "bottomLiftSpeedMmMin",
            "bottomLiftSpeed2MmMin",
        ]
        .iter()
        .copied()
        .any(|key| read_positive_f32(ctb_root, key) || read_positive_f32(export_ctb_root, key));

        let has_explicit_bottom_wait_time = [
            "bottomWaitTimeBeforeCureSec",
            "bottomWaitTimeAfterCureSec",
            "bottomWaitTimeAfterLiftSec",
        ]
        .iter()
        .copied()
        .any(|key| read_positive_f32(ctb_root, key) || read_positive_f32(export_ctb_root, key));

        per_layer_settings = match settings_mode.as_deref() {
            Some("simple") => false,
            Some("twostage") => true,
            _ => has_explicit_two_stage_motion || has_explicit_bottom_wait_time,
        };

        let pls_direct = parse_bool_meta(&meta, "ctb", "perLayerSettings");
        let pls_nested = meta
            .get("export")
            .and_then(|v| v.get("ctb"))
            .and_then(|v| v.get("perLayerSettings"))
            .and_then(Value::as_bool);
        per_layer_settings = pls_direct.or(pls_nested).unwrap_or(per_layer_settings);
    }

    anti_alias_level = anti_alias_level.max(1);

    if let Some((hint_version, _)) = format_hint {
        version = hint_version.clamp(2, 5);
    }

    if version >= 5 && layer_xor_key == 0 {
        layer_xor_key = default_layer_xor_key();
    }

    CtbBuildModel {
        version,
        machine_name,
        bed_size_z_mm,
        created_date_unix,
        modified_date_unix,
        anti_alias_level,
        layer_xor_key,
        projector_type,
        per_layer_settings,
    }
}

pub(super) fn parse_ctb_resin_model_from_job(
    job: &SliceJobV3,
    machine_name: &str,
) -> CtbResinModel {
    let mut resin_name = DEFAULT_RESIN_NAME.to_string();
    let mut resin_type = DEFAULT_RESIN_TYPE.to_string();
    let mut resin_density = DEFAULT_RESIN_DENSITY;
    let mut color_rgba = [0x66, 0x66, 0x66, 0xff];

    if let Some(meta) = parse_json(&job.metadata_json) {
        let material = meta.get("material").and_then(Value::as_object);

        if let Some(v) = material.and_then(|m| m.get("name")).and_then(Value::as_str) {
            resin_name = v.to_string();
        }
        if let Some(v) = material
            .and_then(|m| m.get("resinType"))
            .and_then(Value::as_str)
        {
            resin_type = v.to_string();
        }
        if let Some(v) = material
            .and_then(|m| m.get("resinDensity"))
            .and_then(Value::as_f64)
        {
            resin_density = v as f32;
        }

        if let Some(v) = parse_str_meta(&meta, "ctb", "resinName").or_else(|| {
            meta.get("export")
                .and_then(|v| v.get("ctb"))
                .and_then(|v| v.get("resinName"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        }) {
            resin_name = v;
        }

        if let Some(v) = parse_str_meta(&meta, "ctb", "resinType").or_else(|| {
            meta.get("export")
                .and_then(|v| v.get("ctb"))
                .and_then(|v| v.get("resinType"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        }) {
            resin_type = v;
        }

        if let Some(v) = parse_u32_meta(&meta, "ctb", "resinColorR") {
            color_rgba[0] = v.min(255) as u8;
        }
        if let Some(v) = parse_u32_meta(&meta, "ctb", "resinColorG") {
            color_rgba[1] = v.min(255) as u8;
        }
        if let Some(v) = parse_u32_meta(&meta, "ctb", "resinColorB") {
            color_rgba[2] = v.min(255) as u8;
        }
        if let Some(v) = parse_u32_meta(&meta, "ctb", "resinColorA") {
            color_rgba[3] = v.min(255) as u8;
        }
    }

    if resin_name.is_empty() {
        resin_name = DEFAULT_RESIN_NAME.to_string();
    }
    if resin_type.is_empty() {
        resin_type = DEFAULT_RESIN_TYPE.to_string();
    }

    let _ = machine_name;

    CtbResinModel {
        resin_name,
        resin_type,
        resin_density,
        color_rgba,
    }
}

pub(super) fn parse_machine_software_version(version: u32) -> u32 {
    match version {
        2 => 0x0105_0000,
        3 => 0x0106_0300,
        4 => 0x0109_0000,
        5 => 0x0200_0000,
        _ => 0x0109_0000,
    }
}

pub(super) fn decode_embedded_disclaimer_bytes() -> Result<Vec<u8>, SlicerV3Error> {
    base64::engine::general_purpose::STANDARD
        .decode(CTB_DISCLAIMER_B64)
        .map_err(|e| {
            SlicerV3Error::UnsupportedOutput(format!(
                "failed to decode embedded CTB disclaimer bytes: {e}"
            ))
        })
}
