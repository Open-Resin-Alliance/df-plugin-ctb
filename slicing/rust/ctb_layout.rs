// CTB layout and container building - Refactored into V5 encrypted and non-encrypted variants

use crate::engine::SlicerV3Error;
use crate::types::SliceJobV3;

use super::ctb_metadata::parse_ctb_format_version_hint_from_job;
use super::ctb_types::{CtbPreparedLayer, CTB_PAGE_SIZE};

use super::ctb_encoder::build_ctb_container_bytes_with_progress as build_ctb_v5_with_progress;
use super::ctb_encoder::build_ctb_encrypted_container_bytes_with_progress as build_ctb_v5enc_with_progress;

// === Shared utility functions for both encrypted and non-encrypted ===

pub(super) fn push_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(super) fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_f32(out: &mut Vec<u8>, value: f32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_bytes_padded(out: &mut Vec<u8>, bytes: &[u8], target_len: usize) {
    if bytes.len() >= target_len {
        out.extend_from_slice(&bytes[..target_len]);
        return;
    }

    out.extend_from_slice(bytes);
    out.resize(out.len() + (target_len - bytes.len()), 0);
}

pub(super) fn page_number_and_offset(absolute_offset: u64) -> (u32, u32) {
    let page = (absolute_offset / CTB_PAGE_SIZE) as u32;
    let offset = (absolute_offset - (page as u64) * CTB_PAGE_SIZE) as u32;
    (page, offset)
}

// === Shared RLE encoding and layer handling ===

pub(super) fn rle_encode_mask_row_major(mask: &[u8]) -> Vec<u8> {
    if mask.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(32 * 1024);
    let mut run_value = mask[0];
    let mut run_len = 1u32;

    for &px in &mask[1..] {
        if px == run_value {
            run_len += 1;
        } else {
            push_ctb_run(&mut out, run_len, run_value);
            run_value = px;
            run_len = 1;
        }
    }

    push_ctb_run(&mut out, run_len, run_value);
    out
}

fn rle_encode_thresholded_row_major(mask: &[u8], threshold: u8) -> Vec<u8> {
    if mask.is_empty() {
        return Vec::new();
    }

    // CTB output is highly compressed; start small to avoid massive heap allocations
    // that serialize on the OS allocator lock, causing low CPU utilization.
    let mut out = Vec::with_capacity(32 * 1024);
    let mut run_value = if mask[0] > threshold { 255 } else { 0 };
    let mut run_len = 0u32;
    let mut i = 0;

    let len = mask.len();
    while i < len {
        let chunk = &mask[i..];
        if run_value == 0 {
            if let Some(pos) = chunk.iter().position(|&x| x > threshold) {
                run_len += pos as u32;
                i += pos;
                push_ctb_run(&mut out, run_len, run_value);
                run_value = 255;
                run_len = 0;
            } else {
                run_len += (len - i) as u32;
                break;
            }
        } else {
            if let Some(pos) = chunk.iter().position(|&x| x <= threshold) {
                run_len += pos as u32;
                i += pos;
                push_ctb_run(&mut out, run_len, run_value);
                run_value = 0;
                run_len = 0;
            } else {
                run_len += (len - i) as u32;
                break;
            }
        }
    }

    if run_len > 0 {
        push_ctb_run(&mut out, run_len, run_value);
    }
    out
}

pub(super) fn encode_single_ctb_layer_from_raw_mask(
    layer_index: usize,
    raw_mask: &[u8],
    is_anti_aliased: bool,
    threshold: u8,
    layer_xor_key: u32,
) -> CtbPreparedLayer {
    let mut encoded = if is_anti_aliased {
        rle_encode_mask_row_major(raw_mask)
    } else {
        rle_encode_thresholded_row_major(raw_mask, threshold)
    };

    ctb_layer_rle_xor(layer_xor_key, layer_index as u32, &mut encoded);
    CtbPreparedLayer {
        index: layer_index,
        source_len: raw_mask.len(),
        encoded,
    }
}

pub(super) fn encode_single_ctb_empty_layer(
    layer_index: usize,
    expected_pixels: usize,
    layer_xor_key: u32,
) -> CtbPreparedLayer {
    let mut encoded = Vec::with_capacity(5);
    push_ctb_run(
        &mut encoded,
        expected_pixels.min(u32::MAX as usize) as u32,
        0,
    );
    ctb_layer_rle_xor(layer_xor_key, layer_index as u32, &mut encoded);
    CtbPreparedLayer {
        index: layer_index,
        source_len: expected_pixels,
        encoded,
    }
}

pub(super) fn push_ctb_run(out: &mut Vec<u8>, len: u32, value_8bit: u8) {
    if len == 0 {
        return;
    }

    let mut code = value_8bit >> 1;
    if len > 1 {
        code |= 0x80;
    }
    out.push(code);

    if len <= 1 {
        return;
    }

    if len <= 0x7f {
        out.push(len as u8);
        return;
    }

    if len <= 0x3fff {
        out.push(((len >> 8) as u8) | 0x80);
        out.push(len as u8);
        return;
    }

    if len <= 0x1f_ffff {
        out.push(((len >> 16) as u8) | 0xc0);
        out.push((len >> 8) as u8);
        out.push(len as u8);
        return;
    }

    let clamped = len.min(0x0fff_ffff);
    out.push(((clamped >> 24) as u8) | 0xe0);
    out.push((clamped >> 16) as u8);
    out.push((clamped >> 8) as u8);
    out.push(clamped as u8);
}

#[cfg(test)]
pub(super) fn normalize_to_binary_mask(mask: &[u8], threshold: u8) -> (Vec<u8>, u32) {
    let mut out = Vec::with_capacity(mask.len());
    let mut lit_pixels: u32 = 0;

    for &px in mask {
        let bin = if px > threshold { 255 } else { 0 };
        if bin == 255 {
            lit_pixels = lit_pixels.saturating_add(1);
        }
        out.push(bin);
    }

    (out, lit_pixels)
}

pub(super) fn ctb_layer_rle_xor(seed: u32, layer_index: u32, bytes: &mut [u8]) {
    if seed == 0 {
        return;
    }

    let init = seed.wrapping_mul(0x2d83_cdac).wrapping_add(0xd8a8_3423);
    let mut key = layer_index
        .wrapping_mul(0x1e15_30cd)
        .wrapping_add(0xec3d_47cd)
        .wrapping_mul(init);

    // Fast path: bulk process 4 bytes at a time
    let mut chunks = bytes.chunks_exact_mut(4);
    for chunk in &mut chunks {
        // key is applied byte-by-byte in little-endian order: (key>>0), (key>>8), (key>>16), (key>>24)
        let key_bytes = key.to_le_bytes();
        chunk[0] ^= key_bytes[0];
        chunk[1] ^= key_bytes[1];
        chunk[2] ^= key_bytes[2];
        chunk[3] ^= key_bytes[3];
        key = key.wrapping_add(init);
    }

    // Handle remaining 1-3 bytes
    let remainder = chunks.into_remainder();
    if !remainder.is_empty() {
        let key_bytes = key.to_le_bytes();
        for (i, b) in remainder.iter_mut().enumerate() {
            *b ^= key_bytes[i];
        }
    }
}

pub(super) fn prepare_layers_for_ctb(
    raw_masks: &[Vec<u8>],
    is_anti_aliased: bool,
    threshold: u8,
    layer_xor_key: u32,
) -> Vec<CtbPreparedLayer> {
    prepare_layers_for_ctb_with_progress(raw_masks, is_anti_aliased, threshold, layer_xor_key, None)
}

pub(super) fn prepare_layers_for_ctb_with_progress(
    raw_masks: &[Vec<u8>],
    is_anti_aliased: bool,
    threshold: u8,
    layer_xor_key: u32,
    on_progress: Option<&dyn Fn(u32, u32)>,
) -> Vec<CtbPreparedLayer> {
    let total = raw_masks.len() as u32;
    let mut out = Vec::with_capacity(raw_masks.len());

    for (index, layer) in raw_masks.iter().enumerate() {
        out.push(encode_single_ctb_layer_from_raw_mask(
            index,
            layer,
            is_anti_aliased,
            threshold,
            layer_xor_key,
        ));

        if let Some(progress) = on_progress {
            progress((index as u32) + 1, total.max(1));
        }
    }

    out
}

// === Public Dispatcher ===
// Routes to v5 or v5enc implementations based on metadata

pub(super) fn build_ctb_container_bytes(
    job: &SliceJobV3,
    prepared: &[CtbPreparedLayer],
) -> Result<Vec<u8>, SlicerV3Error> {
    build_ctb_container_bytes_with_progress(job, prepared, None)
}

pub(super) fn build_ctb_container_bytes_with_progress(
    job: &SliceJobV3,
    prepared: &[CtbPreparedLayer],
    on_progress: Option<&dyn Fn(u32, u32)>,
) -> Result<Vec<u8>, SlicerV3Error> {
    let schedule = super::ctb_encoder::compile_layer_settings(job)?;
    let force_encrypted = parse_ctb_format_version_hint_from_job(job)
        .map(|(_, is_encrypted)| is_encrypted)
        .unwrap_or(false);

    if force_encrypted {
        build_ctb_v5enc_with_progress(job, prepared, &schedule, on_progress)
    } else {
        build_ctb_v5_with_progress(job, prepared, &schedule, on_progress)
    }
}
