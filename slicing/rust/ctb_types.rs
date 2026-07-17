pub(super) const DEFAULT_BINARY_THRESHOLD: u8 = 127;
pub(super) const DEFAULT_CTB_VERSION: u32 = 5;
pub(super) const DEFAULT_MACHINE_NAME: &str = "DragonFruit CTB";
pub(super) const DEFAULT_RESIN_NAME: &str = "DragonFruit Resin";
pub(super) const DEFAULT_RESIN_TYPE: &str = "Standard";
pub(super) const DEFAULT_RESIN_DENSITY: f32 = 1.1;

pub(super) const CTB_MAGIC_V2_V3: u32 = 0x12FD_0086;
pub(super) const CTB_MAGIC_V4_V5: u32 = 0x12FD_0106;
pub(super) const CTB_MAGIC_V5_ENCRYPTED: u32 = 0x12FD_0107;

pub(super) const CTB_HEADER_SIZE: u32 = 112;
pub(super) const CTB_PREVIEW_RECORD_SIZE: u32 = 32;
pub(super) const CTB_PRINT_PARAMETERS_SIZE: u32 = 60;
pub(super) const CTB_SLICER_INFO_FIXED_SIZE: u32 = 76;
pub(super) const CTB_PRINT_PARAMETERS_V4_SIZE: u32 = 464;
pub(super) const CTB_PRINT_PARAMETERS_V4_RESERVED_SIZE: usize = 380;
pub(super) const CTB_LAYER_DEF_SIZE: u32 = 36;
pub(super) const CTB_LAYER_DEF_EX_SIZE: u32 = 84;
pub(super) const CTB_PAGE_SIZE: u64 = 4_294_967_296;
pub(super) const CTB_ENCRYPTED_HEADER_SIZE: u32 = 48;
pub(super) const CTB_ENCRYPTED_SETTINGS_SIZE: u32 = 288;
pub(super) const CTB_ENCRYPTED_SETTINGS_OFFSET: u32 = 48;
pub(super) const CTB_ENCRYPTED_LAYER_DEF_SIZE: u32 = 88;

pub(super) const CTB_DISCLAIMER_SIZE: usize = 320;
pub(super) const CTB_DISCLAIMER_B64: &str = "TGF5b3V0IGFuZCByZWNvcmQgZm9ybWF0IGZvciB0aGUgY3RiIGFuZCBjYmRkbHAgZmlsZSB0eXBlcyBhcmUgdGhlIGNvcHlyaWdodGVkIHByb2dyYW1zIG9yIGNvZGVzIG9mIENCRCBUZWNobm9sb2d5IChDaGluYSkgSW5jLi5UaGUgQ3VzdG9tZXIgb3IgVXNlciBzaGFsbCBub3QgaW4gYW55IG1hbm5lciByZXByb2R1Y2UsIGRpc3RyaWJ1dGUsIG1vZGlmeSwgZGVjb21waWxlLCBkaXNhc3NlbWJsZSwgZGVjcnlwdCwgZXh0cmFjdCwgcmV2ZXJzZSBlbmdpbmVlciwgbGVhc2UsIGFzc2lnbiwgb3Igc3VibGljZW5zZSB0aGUgc2FpZCBwcm9ncmFtcyBvciBjb2Rlcy4=";

pub(super) const PREVIEW_LARGE_W: u32 = 400;
pub(super) const PREVIEW_LARGE_H: u32 = 300;
pub(super) const PREVIEW_SMALL_W: u32 = 200;
pub(super) const PREVIEW_SMALL_H: u32 = 125;
pub(super) const PREVIEW_REPEAT_RGB15_MASK: u16 = 0x20;
pub(super) const PREVIEW_RLE16_ENCODING_LIMIT: u32 = 0x0fff;

#[derive(Debug, Clone)]
pub(super) struct CtbPreparedLayer {
    pub index: usize,
    pub source_len: usize,
    pub encoded: Vec<u8>,
}

/// Lightweight per-layer bookkeeping used by the streaming encoder: everything
/// `write_layer_def_ex` needs to build the layer descriptor table, without
/// retaining the (potentially huge) encoded payload bytes in memory.
#[derive(Debug, Clone, Copy)]
pub(super) struct CtbLayerRecord {
    pub index: usize,
    pub source_len: usize,
    pub encoded_len: u32,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CtbTimingModel {
    pub normal_exposure_sec: f32,
    pub bottom_exposure_sec: f32,
    pub light_off_delay_sec: f32,
    pub bottom_light_off_delay_sec: f32,
    pub bottom_layer_count: u32,
    pub lift_distance_mm: f32,
    pub lift_distance2_mm: f32,
    pub lift_speed_mm_min: f32,
    pub lift_speed2_mm_min: f32,
    pub retract_distance_mm: f32,
    pub retract_distance2_mm: f32,
    pub retract_speed_mm_min: f32,
    pub retract_speed2_mm_min: f32,
    pub bottom_lift_distance_mm: f32,
    pub bottom_lift_distance2_mm: f32,
    pub bottom_lift_speed_mm_min: f32,
    pub bottom_lift_speed2_mm_min: f32,
    pub bottom_retract_speed_mm_min: f32,
    pub bottom_retract_speed2_mm_min: f32,
    pub bottom_retract_height2_mm: f32,
    pub transition_layer_count: u32,
    pub wait_time_before_cure_sec: f32,
    pub wait_time_after_cure_sec: f32,
    pub wait_time_after_lift_sec: f32,
    pub wait_time_bottom_layer_count: u32,
    pub bottom_wait_time_before_cure_sec: f32,
    pub bottom_wait_time_after_cure_sec: f32,
    pub bottom_wait_time_after_lift_sec: f32,
    pub projector_duty_cycle_pwm: u16,
    pub bottom_layer_projector_duty_cycle_pwm: u16,
}

#[derive(Debug, Clone)]
pub(super) struct CtbBuildModel {
    pub version: u32,
    pub machine_name: String,
    pub bed_size_z_mm: f32,
    pub created_date_unix: u32,
    pub modified_date_unix: u32,
    pub anti_alias_level: u32,
    pub layer_xor_key: u32,
    pub projector_type: u32,
    pub per_layer_settings: bool,
}

#[derive(Debug, Clone)]
pub(super) struct CtbResinModel {
    pub resin_name: String,
    pub resin_type: String,
    pub resin_density: f32,
    pub color_rgba: [u8; 4],
}

#[derive(Debug, Clone)]
pub(super) struct CtbPreviewBlob {
    pub width: u32,
    pub height: u32,
    pub encoded: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CtbPreviewOffsets {
    pub large_record_offset: u32,
    pub small_record_offset: u32,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CtbExtendedOffsets {
    pub disclaimer_offset: u32,
    pub disclaimer_length: u32,
    pub print_parameters_v4_offset: u32,
    pub resin_parameters_offset: u32,
}

#[derive(Debug, Clone)]
pub(super) struct CtbResinPayload {
    pub machine_name_bytes: Vec<u8>,
    pub resin_type_bytes: Vec<u8>,
    pub resin_name_bytes: Vec<u8>,
    pub tail_padding_bytes: usize,
}
