mod ctb_crypto;
mod ctb_layout;
mod ctb_metadata;
mod ctb_preview;
mod ctb_types;
mod ctb_wait_profile;
mod ctb_first_layer;
mod ctb_empty_layer;
//mod ctb_v5;
//mod ctb_v5enc;
mod ctb_encoder;

use crate::encoders::FormatEncoder;
use crate::encoders::RawMaskStreamEncoder;
use crate::encoders::RleStreamEncoder;
use crate::engine::SlicerV3Error;
use crate::types::{LayerAreaStatsV3, RenderedLayersV3, SliceJobV3};
use crossbeam_channel::bounded;
use ctb_layout::{
    build_ctb_container_bytes, build_ctb_container_bytes_with_progress,
    encode_single_ctb_empty_layer, encode_single_ctb_layer_from_raw_mask, prepare_layers_for_ctb,
    prepare_layers_for_ctb_with_progress,
};
use ctb_metadata::{parse_ctb_build_model_from_job, parse_threshold_from_metadata};
use std::path::Path;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

#[cfg(test)]
use ctb_layout::{
    ctb_layer_rle_xor, normalize_to_binary_mask, push_ctb_run, rle_encode_mask_row_major,
};
#[cfg(test)]
use ctb_metadata::decode_embedded_disclaimer_bytes;
#[cfg(test)]
use ctb_types::{CtbPreparedLayer, CTB_DISCLAIMER_SIZE, CTB_HEADER_SIZE};

pub struct CtbPluginEncoder;

fn choose_ctb_encode_threads() -> usize {
    let hw = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let env = std::env::var("DF_V3_CTB_ENCODE_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(hw);
    env.clamp(1, hw)
}

fn cap_ctb_encode_workers_for_mask_bytes(requested: usize, expected_pixels: usize) -> usize {
    let bytes_per_mask = expected_pixels;
    let mut capped = requested.max(1);

    // Optional override: memory budget for in-flight CTB raw masks (MB).
    // Example: 1024 means allow about 1 GB worth of queued/working masks.
    let budget_override = std::env::var("DF_V3_MAX_CTB_INFLIGHT_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 64)
        .map(|mb| mb.saturating_mul(1024 * 1024));

    if let Some(budget_bytes) = budget_override {
        let allowed = (budget_bytes / bytes_per_mask.max(1)).max(1);
        capped = capped.min(allowed);
    }

    // Each encoder worker can hold a full raw mask plus encoded output buffers.
    // Be conservative for massive layers to prevent allocation failures.
    if budget_override.is_none() {
        if bytes_per_mask >= 48 * 1024 * 1024 {
            capped = capped.min(2);
        } else if bytes_per_mask >= 24 * 1024 * 1024 {
            capped = capped.min(4);
        } else if bytes_per_mask >= 12 * 1024 * 1024 {
            capped = capped.min(8);
        }
    }

    capped.max(1)
}

fn choose_ctb_encode_queue_depth(worker_count: usize, expected_pixels: usize) -> usize {
    let bytes_per_mask = expected_pixels;
    if let Some(budget_bytes) = std::env::var("DF_V3_MAX_CTB_INFLIGHT_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 64)
        .map(|mb| mb.saturating_mul(1024 * 1024))
    {
        let allowed = (budget_bytes / bytes_per_mask.max(1)).max(1);
        return allowed.min((worker_count.saturating_mul(2)).max(1));
    }

    if bytes_per_mask >= 48 * 1024 * 1024 {
        1
    } else if bytes_per_mask >= 24 * 1024 * 1024 {
        2
    } else if bytes_per_mask >= 12 * 1024 * 1024 {
        3
    } else {
        (worker_count.saturating_mul(3)).clamp(3, 24)
    }
}

struct CtbRawMaskStreamingEncoder {
    job: SliceJobV3,
    work_tx: Option<crossbeam_channel::Sender<(u32, Vec<u8>)>>,
    result_rx: mpsc::Receiver<Result<ctb_types::CtbPreparedLayer, SlicerV3Error>>,
    workers: Vec<thread::JoinHandle<()>>,
    consumed_layers: u32,
}

impl RawMaskStreamEncoder for CtbRawMaskStreamingEncoder {
    fn consume_raw_mask_layer(
        &mut self,
        layer_index: u32,
        raw_mask: Vec<u8>,
    ) -> Result<(), SlicerV3Error> {
        let Some(ref tx) = self.work_tx else {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "CTB streaming encoder no longer accepts layers after finalize".to_string(),
            ));
        };

        tx.send((layer_index, raw_mask)).map_err(|_| {
            SlicerV3Error::MissingRenderedLayerPayload(
                "CTB streaming worker channel closed unexpectedly".to_string(),
            )
        })?;
        self.consumed_layers = self.consumed_layers.saturating_add(1);
        Ok(())
    }

    fn finalize_to_bytes(mut self: Box<Self>) -> Result<Vec<u8>, SlicerV3Error> {
        if self.consumed_layers == 0 {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "no rendered layers were provided for CTB encoding".to_string(),
            ));
        }

        // Close producer channel and let workers drain outstanding tasks.
        let _ = self.work_tx.take();

        while let Some(handle) = self.workers.pop() {
            if handle.join().is_err() {
                return Err(SlicerV3Error::UnsupportedOutput(
                    "CTB streaming worker panicked".to_string(),
                ));
            }
        }

        let expected_layers = self.consumed_layers as usize;
        let mut ordered: Vec<Option<ctb_types::CtbPreparedLayer>> =
            Vec::with_capacity(expected_layers);
        ordered.resize_with(expected_layers, || None);

        for _ in 0..expected_layers {
            let msg = self.result_rx.recv().map_err(|_| {
                SlicerV3Error::MissingRenderedLayerPayload(
                    "CTB streaming worker results ended unexpectedly".to_string(),
                )
            })?;

            let prepared = msg?;
            if prepared.index >= expected_layers {
                return Err(SlicerV3Error::MissingRenderedLayerPayload(format!(
                    "CTB worker emitted out-of-range layer index {} (expected < {})",
                    prepared.index, expected_layers
                )));
            }
            let index = prepared.index;
            if ordered[index].is_some() {
                return Err(SlicerV3Error::MissingRenderedLayerPayload(format!(
                    "CTB worker emitted duplicate layer index {}",
                    index
                )));
            }
            ordered[index] = Some(prepared);
        }

        let mut prepared = Vec::with_capacity(expected_layers);
        for (index, layer) in ordered.into_iter().enumerate() {
            let Some(layer) = layer else {
                return Err(SlicerV3Error::MissingRenderedLayerPayload(format!(
                    "CTB layer {} missing from streaming worker output",
                    index
                )));
            };
            prepared.push(layer);
        }

        build_ctb_container_bytes(&self.job, &prepared)
    }
}

/// Sequential RLE streaming encoder: receives `Vec<RleRun>` per layer (already
/// rasterized by `rasterize_layer_rle`), converts directly to CTB RLE, and
/// assembles the container in `finalize_to_bytes` — zero pixel-buffer overhead.
struct CtbRleStreamingEncoder {
    job: SliceJobV3,
    layer_xor_key: u32,
    is_anti_aliased: bool,
    threshold: u8,
    total_pixels: usize,
    prepared: Vec<ctb_types::CtbPreparedLayer>,
}

impl RleStreamEncoder for CtbRleStreamingEncoder {
    fn consume_rle_layer(
        &mut self,
        layer_index: u32,
        runs: Vec<crate::rle::RleRun>,
    ) -> Result<(), SlicerV3Error> {
        let mut encoded = Vec::with_capacity(32 * 1024);

        if runs.is_empty() {
            // All-black layer: single zero run.
            ctb_layout::push_ctb_run(
                &mut encoded,
                self.total_pixels.min(u32::MAX as usize) as u32,
                0,
            );
        } else {
            for run in &runs {
                let value = if self.is_anti_aliased {
                    run.value
                } else {
                    // Apply threshold: rasterizer may produce intermediate values
                    // in degenerate cases; snap to binary for non-AA CTB output.
                    if run.value > self.threshold {
                        255
                    } else {
                        0
                    }
                };
                ctb_layout::push_ctb_run(&mut encoded, run.length, value);
            }
        }

        ctb_layout::ctb_layer_rle_xor(self.layer_xor_key, layer_index, &mut encoded);
        self.prepared.push(ctb_types::CtbPreparedLayer {
            index: layer_index as usize,
            source_len: self.total_pixels,
            encoded,
        });
        Ok(())
    }

    fn finalize_to_bytes(mut self: Box<Self>) -> Result<Vec<u8>, SlicerV3Error> {
        if self.prepared.is_empty() {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "no rendered layers were provided for CTB RLE encoding".to_string(),
            ));
        }
        self.prepared.sort_unstable_by_key(|p| p.index);
        build_ctb_container_bytes(&self.job, &self.prepared)
    }

    fn parallel_encode_fn(
        &self,
    ) -> Option<
        Arc<dyn Fn(u32, &[crate::rle::RleRun]) -> Result<Vec<u8>, SlicerV3Error> + Send + Sync>,
    > {
        let layer_xor_key = self.layer_xor_key;
        let is_anti_aliased = self.is_anti_aliased;
        let threshold = self.threshold;
        let total_pixels = self.total_pixels;

        Some(Arc::new(
            move |layer_index: u32, runs: &[crate::rle::RleRun]| {
                let mut encoded = Vec::with_capacity(32 * 1024);

                if runs.is_empty() {
                    ctb_layout::push_ctb_run(
                        &mut encoded,
                        total_pixels.min(u32::MAX as usize) as u32,
                        0,
                    );
                } else {
                    for run in runs {
                        let value = if is_anti_aliased {
                            run.value
                        } else {
                            if run.value > threshold {
                                255
                            } else {
                                0
                            }
                        };
                        ctb_layout::push_ctb_run(&mut encoded, run.length, value);
                    }
                }

                ctb_layout::ctb_layer_rle_xor(layer_xor_key, layer_index, &mut encoded);
                Ok(encoded)
            },
        ))
    }

    fn store_encoded_layer(&mut self, layer_index: u32, bytes: Vec<u8>) {
        self.prepared.push(ctb_types::CtbPreparedLayer {
            index: layer_index as usize,
            source_len: self.total_pixels,
            encoded: bytes,
        });
    }
}

pub fn create_plugin_encoder() -> Vec<Box<dyn FormatEncoder>> {
    vec![Box::new(CtbPluginEncoder)]
}

// Parsing and layout logic live in `ctb_metadata.rs` and `ctb_layout.rs`.

impl FormatEncoder for CtbPluginEncoder {
    fn output_format(&self) -> &'static str {
        ".ctb"
    }

    fn requires_png_layers(&self) -> bool {
        false
    }

    fn requires_raw_mask_layers(&self) -> bool {
        true
    }

    fn create_raw_mask_stream_encoder(
        &self,
        job: &SliceJobV3,
    ) -> Result<Option<Box<dyn RawMaskStreamEncoder>>, SlicerV3Error> {
        let threshold = parse_threshold_from_metadata(&job.metadata_json);
        let build = parse_ctb_build_model_from_job(job);
        let expected_pixels =
            (job.source_width_px as usize).saturating_mul(job.source_height_px as usize);

        let worker_count =
            cap_ctb_encode_workers_for_mask_bytes(choose_ctb_encode_threads(), expected_pixels);
        let queue_depth = choose_ctb_encode_queue_depth(worker_count, expected_pixels);
        let (work_tx, work_rx) = bounded::<(u32, Vec<u8>)>(queue_depth);
        let (result_tx, result_rx) =
            mpsc::channel::<Result<ctb_types::CtbPreparedLayer, SlicerV3Error>>();
        let mut workers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            let worker_threshold = threshold;
            let worker_layer_xor_key = build.layer_xor_key;
            let worker_expected_pixels = expected_pixels;
            let worker_is_anti_aliased = job.produces_grayscale_output();

            let handle = thread::spawn(move || loop {
                let task = work_rx.recv();

                let Ok((layer_index, raw_mask)) = task else {
                    break;
                };

                if raw_mask.is_empty() {
                    let prepared = encode_single_ctb_empty_layer(
                        layer_index as usize,
                        worker_expected_pixels,
                        worker_layer_xor_key,
                    );
                    crate::pipeline::return_mask_to_pool(raw_mask);

                    if result_tx.send(Ok(prepared)).is_err() {
                        break;
                    }
                    continue;
                }

                if raw_mask.len() != worker_expected_pixels {
                    let len = raw_mask.len();
                    crate::pipeline::return_mask_to_pool(raw_mask);
                    let _ =
                        result_tx.send(Err(SlicerV3Error::MissingRenderedLayerPayload(format!(
                            "CTB layer {layer_index} size mismatch: expected {} bytes, got {}",
                            worker_expected_pixels, len
                        ))));
                    continue;
                }

                let prepared = encode_single_ctb_layer_from_raw_mask(
                    layer_index as usize,
                    &raw_mask,
                    worker_is_anti_aliased,
                    worker_threshold,
                    worker_layer_xor_key,
                );
                crate::pipeline::return_mask_to_pool(raw_mask);

                if result_tx.send(Ok(prepared)).is_err() {
                    break;
                }
            });

            workers.push(handle);
        }
        drop(result_tx);

        Ok(Some(Box::new(CtbRawMaskStreamingEncoder {
            job: job.clone(),
            work_tx: Some(work_tx),
            result_rx,
            workers,
            consumed_layers: 0,
        })))
    }

    fn create_rle_stream_encoder(
        &self,
        job: &SliceJobV3,
    ) -> Result<Option<Box<dyn RleStreamEncoder>>, SlicerV3Error> {
        let build = parse_ctb_build_model_from_job(job);
        let threshold = parse_threshold_from_metadata(&job.metadata_json);
        let is_anti_aliased = job.produces_grayscale_output();
        let total_pixels =
            (job.source_width_px as usize).saturating_mul(job.source_height_px as usize);
        Ok(Some(Box::new(CtbRleStreamingEncoder {
            job: job.clone(),
            layer_xor_key: build.layer_xor_key,
            is_anti_aliased,
            threshold,
            total_pixels,
            prepared: Vec::with_capacity(job.total_layers as usize),
        })))
    }

    fn estimate_encode_progress_units(&self, rendered_layers: &RenderedLayersV3) -> u32 {
        let layers = rendered_layers
            .raw_mask_layers
            .as_ref()
            .map(|v| v.len() as u32)
            .unwrap_or(0);
        layers.saturating_mul(2).saturating_add(1).max(1)
    }

    fn encode_container_from_rendered_layers_with_progress(
        &self,
        job: &SliceJobV3,
        rendered_layers: &RenderedLayersV3,
        _layer_area_stats: &[LayerAreaStatsV3],
        on_progress: Option<&dyn Fn(u32, u32)>,
    ) -> Result<Vec<u8>, SlicerV3Error> {
        let Some(raw_masks) = rendered_layers.raw_mask_layers.as_ref() else {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "raw mask layers are required for CTB encoding".to_string(),
            ));
        };

        if raw_masks.is_empty() {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "no rendered layers were provided for CTB encoding".to_string(),
            ));
        }

        let expected_pixels =
            (job.source_width_px as usize).saturating_mul(job.source_height_px as usize);
        for (idx, layer) in raw_masks.iter().enumerate() {
            if layer.len() != expected_pixels {
                return Err(SlicerV3Error::MissingRenderedLayerPayload(format!(
                    "CTB layer {idx} size mismatch: expected {expected_pixels} bytes, got {}",
                    layer.len()
                )));
            }
        }

        let threshold = parse_threshold_from_metadata(&job.metadata_json);
        let build = parse_ctb_build_model_from_job(job);

        let total_prepare = raw_masks.len() as u32;
        let total_layout = raw_masks.len() as u32;
        let total_progress = total_prepare
            .saturating_add(total_layout)
            .saturating_add(1)
            .max(1);

        let prepare_progress = on_progress.map(|progress| {
            move |done: u32, total: u32| {
                let safe_total = total.max(1);
                let mapped = ((done.min(safe_total) as u64) * (total_prepare as u64)
                    / (safe_total as u64)) as u32;
                progress(mapped, total_progress);
            }
        });

        let is_anti_aliased = job.produces_grayscale_output();
        let prepared = prepare_layers_for_ctb_with_progress(
            raw_masks,
            is_anti_aliased,
            threshold,
            build.layer_xor_key,
            prepare_progress.as_ref().map(|cb| cb as &dyn Fn(u32, u32)),
        );

        let source_bytes: usize = prepared.iter().map(|l| l.source_len).sum();
        let encoded_bytes: usize = prepared.iter().map(|l| l.encoded.len()).sum();
        if encoded_bytes == 0 {
            return Err(SlicerV3Error::UnsupportedOutput(format!(
                "CTB encoding produced empty payload (source bytes: {source_bytes})"
            )));
        }

        let layout_progress = on_progress.map(|progress| {
            move |done: u32, total: u32| {
                let safe_total = total.max(1);
                let mapped = ((done.min(safe_total) as u64) * (total_layout as u64)
                    / (safe_total as u64)) as u32;
                progress(total_prepare.saturating_add(mapped), total_progress);
            }
        });

        let bytes = build_ctb_container_bytes_with_progress(
            job,
            &prepared,
            layout_progress.as_ref().map(|cb| cb as &dyn Fn(u32, u32)),
        )?;

        if let Some(progress) = on_progress {
            progress(total_progress, total_progress);
        }

        Ok(bytes)
    }

    fn encode_container_from_rendered_layers(
        &self,
        job: &SliceJobV3,
        rendered_layers: &RenderedLayersV3,
        _layer_area_stats: &[LayerAreaStatsV3],
    ) -> Result<Vec<u8>, SlicerV3Error> {
        let Some(raw_masks) = rendered_layers.raw_mask_layers.as_ref() else {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "raw mask layers are required for CTB encoding".to_string(),
            ));
        };

        if raw_masks.is_empty() {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "no rendered layers were provided for CTB encoding".to_string(),
            ));
        }

        let expected_pixels =
            (job.source_width_px as usize).saturating_mul(job.source_height_px as usize);
        for (idx, layer) in raw_masks.iter().enumerate() {
            if layer.len() != expected_pixels {
                return Err(SlicerV3Error::MissingRenderedLayerPayload(format!(
                    "CTB layer {idx} size mismatch: expected {expected_pixels} bytes, got {}",
                    layer.len()
                )));
            }
        }

        let threshold = parse_threshold_from_metadata(&job.metadata_json);
        let build = parse_ctb_build_model_from_job(job);
        let is_anti_aliased = job.produces_grayscale_output();
        let prepared =
            prepare_layers_for_ctb(raw_masks, is_anti_aliased, threshold, build.layer_xor_key);

        let source_bytes: usize = prepared.iter().map(|l| l.source_len).sum();
        let encoded_bytes: usize = prepared.iter().map(|l| l.encoded.len()).sum();
        if encoded_bytes == 0 {
            return Err(SlicerV3Error::UnsupportedOutput(format!(
                "CTB encoding produced empty payload (source bytes: {source_bytes})"
            )));
        }

        build_ctb_container_bytes(job, &prepared)
    }

    fn encode_container_to_path(
        &self,
        job: &SliceJobV3,
        rendered_layers: &RenderedLayersV3,
        layer_area_stats: &[LayerAreaStatsV3],
        output_path: &Path,
    ) -> Result<(), SlicerV3Error> {
        let bytes =
            self.encode_container_from_rendered_layers(job, rendered_layers, layer_area_stats)?;
        std::fs::write(output_path, bytes)?;
        Ok(())
    }

    fn read_layer_preview_png(
        &self,
        path: &Path,
        layer_number: u32,
    ) -> Result<Vec<u8>, SlicerV3Error> {
        self::read_layer_preview_png(path, layer_number).map_err(SlicerV3Error::LayerPreview)
    }
}

/// Reads a single layer preview PNG from a CTB binary file.
/// `layer_number` is 1-based. Supports non-encrypted CTB V2–V5 and encrypted V5.
pub fn read_layer_preview_png(path: &Path, layer_number: u32) -> Result<Vec<u8>, String> {
    use std::io::Read;

    if layer_number == 0 {
        return Err("Layer number must be >= 1".to_string());
    }

    let mut file =
        std::fs::File::open(path).map_err(|e| format!("Failed opening CTB file: {e}"))?;

    let mut magic_bytes = [0u8; 4];
    file.read_exact(&mut magic_bytes)
        .map_err(|e| format!("CTB magic read failed: {e}"))?;
    let magic = u32::from_le_bytes(magic_bytes);

    if magic == ctb_types::CTB_MAGIC_V5_ENCRYPTED {
        read_encrypted_layer_preview(&mut file, layer_number)
    } else {
        read_plain_layer_preview(&mut file, layer_number)
    }
}

/// Reads a layer preview from a non-encrypted CTB file (V2–V5).
fn read_plain_layer_preview(
    file: &mut std::fs::File,
    layer_number: u32,
) -> Result<Vec<u8>, String> {
    use std::io::{Read, Seek, SeekFrom};

    // Re-read the full 112-byte header from the start.
    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("CTB header seek failed: {e}"))?;

    let mut header = [0u8; ctb_types::CTB_HEADER_SIZE as usize];
    file.read_exact(&mut header)
        .map_err(|e| format!("CTB header read failed: {e}"))?;

    let width_px = u32::from_le_bytes(header[52..56].try_into().unwrap());
    let height_px = u32::from_le_bytes(header[56..60].try_into().unwrap());
    let layers_def_off = u32::from_le_bytes(header[64..68].try_into().unwrap());
    let layer_count = u32::from_le_bytes(header[68..72].try_into().unwrap());
    let xor_key = u32::from_le_bytes(header[100..104].try_into().unwrap());

    if width_px == 0 || height_px == 0 {
        return Err(format!(
            "CTB file reports invalid dimensions {width_px}×{height_px}"
        ));
    }
    if layer_number > layer_count {
        return Err(format!(
            "Layer {layer_number} out of range (file has {layer_count} layers)"
        ));
    }

    let layer_index = layer_number - 1;

    // Read layer def record (36 bytes each).
    let def_offset =
        layers_def_off as u64 + layer_index as u64 * ctb_types::CTB_LAYER_DEF_SIZE as u64;
    file.seek(SeekFrom::Start(def_offset))
        .map_err(|e| format!("CTB layer def seek failed: {e}"))?;

    let mut layer_def = [0u8; ctb_types::CTB_LAYER_DEF_SIZE as usize];
    file.read_exact(&mut layer_def)
        .map_err(|e| format!("CTB layer def read failed: {e}"))?;

    // Plain layer def v4/v5 layout:
    //   [0..4]   position_z_mm
    //   [4..8]   exposure_sec
    //   [8..12]  light_off_sec
    //   [12..16] page-relative data offset
    //   [16..20] encoded data size
    //   [20..24] page number
    //   [24..28] table_size
    //   [28..32] 0
    //   [32..36] 0
    let data_page_rel = u32::from_le_bytes(layer_def[12..16].try_into().unwrap());
    let encoded_len = u32::from_le_bytes(layer_def[16..20].try_into().unwrap());
    let page_number = u32::from_le_bytes(layer_def[20..24].try_into().unwrap());

    let abs_data = page_number as u64 * ctb_types::CTB_PAGE_SIZE + data_page_rel as u64;
    file.seek(SeekFrom::Start(abs_data))
        .map_err(|e| format!("CTB layer data seek failed: {e}"))?;

    let mut rle_bytes = vec![0u8; encoded_len as usize];
    file.read_exact(&mut rle_bytes)
        .map_err(|e| format!("CTB layer RLE read failed: {e}"))?;

    ctb_layout::ctb_layer_rle_xor(xor_key, layer_index, &mut rle_bytes);

    let expected_pixels = width_px as usize * height_px as usize;
    let pixels = decode_ctb_rle(&rle_bytes, expected_pixels);
    encode_pixels_as_grayscale_png(width_px, height_px, &pixels)
}

/// Reads a layer preview from an encrypted CTB V5 file.
fn read_encrypted_layer_preview(
    file: &mut std::fs::File,
    layer_number: u32,
) -> Result<Vec<u8>, String> {
    use std::io::{Read, Seek, SeekFrom};

    // Decrypt the settings block to obtain layout parameters.
    file.seek(SeekFrom::Start(
        ctb_types::CTB_ENCRYPTED_SETTINGS_OFFSET as u64,
    ))
    .map_err(|e| format!("CTB encrypted settings seek failed: {e}"))?;

    let mut settings = vec![0u8; ctb_types::CTB_ENCRYPTED_SETTINGS_SIZE as usize];
    file.read_exact(&mut settings)
        .map_err(|e| format!("CTB encrypted settings read failed: {e}"))?;

    let (key, iv) = ctb_crypto::ctb_default_key_iv();
    ctb_crypto::ctb_decrypt_in_place_no_padding(&mut settings, &key, &iv)
        .map_err(|e| format!("CTB settings AES decrypt failed: {e}"))?;

    // Decrypted settings field layout (see ctb_v5enc.rs for the write-side reference):
    //   [0..8]     checksum (u64)
    //   [8..12]    layer_pointers_offset (u32)
    //   [56..60]   source_width_px (u32)
    //   [60..64]   source_height_px (u32)
    //   [64..68]   layer_count (u32)
    //   [128..132] layer_xor_key (u32)
    let layer_pointers_off = u32::from_le_bytes(settings[8..12].try_into().unwrap());
    let width_px = u32::from_le_bytes(settings[56..60].try_into().unwrap());
    let height_px = u32::from_le_bytes(settings[60..64].try_into().unwrap());
    let layer_count = u32::from_le_bytes(settings[64..68].try_into().unwrap());
    let xor_key = u32::from_le_bytes(settings[128..132].try_into().unwrap());

    if width_px == 0 || height_px == 0 {
        return Err(format!(
            "CTB encrypted file reports invalid dimensions {width_px}×{height_px}"
        ));
    }
    if layer_number > layer_count {
        return Err(format!(
            "Layer {layer_number} out of range (file has {layer_count} layers)"
        ));
    }

    let layer_index = layer_number - 1;

    // Each pointer table entry is 16 bytes:
    //   [0..4]   page-relative layer def offset
    //   [4..8]   page number of layer def
    //   [8..12]  def size (CTB_ENCRYPTED_LAYER_DEF_SIZE)
    //   [12..16] 0
    let ptr_entry_off = layer_pointers_off as u64 + layer_index as u64 * 16;
    file.seek(SeekFrom::Start(ptr_entry_off))
        .map_err(|e| format!("CTB pointer table seek failed: {e}"))?;

    let mut pointer = [0u8; 16];
    file.read_exact(&mut pointer)
        .map_err(|e| format!("CTB pointer table read failed: {e}"))?;

    let def_page_rel = u32::from_le_bytes(pointer[0..4].try_into().unwrap());
    let def_page = u32::from_le_bytes(pointer[4..8].try_into().unwrap());
    let abs_def = def_page as u64 * ctb_types::CTB_PAGE_SIZE + def_page_rel as u64;

    file.seek(SeekFrom::Start(abs_def))
        .map_err(|e| format!("CTB encrypted layer def seek failed: {e}"))?;

    // Encrypted layer def is 88 bytes. Layout:
    //   [0..4]   table_size (CTB_ENCRYPTED_LAYER_DEF_SIZE = 88)
    //   [4..8]   position_z_mm
    //   [8..12]  exposure_sec
    //   [12..16] light_off_sec
    //   [16..20] page-relative data offset
    //   [20..24] page number of data
    //   [24..28] encoded data size in bytes
    //   ... (timing fields, not needed for preview)
    let mut layer_def = [0u8; ctb_types::CTB_ENCRYPTED_LAYER_DEF_SIZE as usize];
    file.read_exact(&mut layer_def)
        .map_err(|e| format!("CTB encrypted layer def read failed: {e}"))?;

    let data_page_rel = u32::from_le_bytes(layer_def[16..20].try_into().unwrap());
    let data_page = u32::from_le_bytes(layer_def[20..24].try_into().unwrap());
    let encoded_len = u32::from_le_bytes(layer_def[24..28].try_into().unwrap());
    let abs_data = data_page as u64 * ctb_types::CTB_PAGE_SIZE + data_page_rel as u64;

    file.seek(SeekFrom::Start(abs_data))
        .map_err(|e| format!("CTB encrypted layer data seek failed: {e}"))?;

    let mut rle_bytes = vec![0u8; encoded_len as usize];
    file.read_exact(&mut rle_bytes)
        .map_err(|e| format!("CTB encrypted layer RLE read failed: {e}"))?;

    ctb_layout::ctb_layer_rle_xor(xor_key, layer_index, &mut rle_bytes);

    let expected_pixels = width_px as usize * height_px as usize;
    let pixels = decode_ctb_rle(&rle_bytes, expected_pixels);
    encode_pixels_as_grayscale_png(width_px, height_px, &pixels)
}

/// Decodes CTB run-length encoded data into a flat grayscale pixel buffer.
fn decode_ctb_rle(data: &[u8], expected_pixels: usize) -> Vec<u8> {
    let mut pixels = Vec::with_capacity(expected_pixels);
    let mut i = 0;

    while i < data.len() && pixels.len() < expected_pixels {
        let code = data[i];
        i += 1;
        // Low 7 bits hold (pixel_value >> 1); high bit signals a length field follows.
        let pixel = (code & 0x7f) << 1;
        let has_len = (code & 0x80) != 0;

        let run_len: u32 = if !has_len {
            1
        } else if i >= data.len() {
            break;
        } else {
            let b0 = data[i];
            i += 1;
            if b0 & 0x80 == 0 {
                // 1-byte length: 0x00..0x7F
                b0 as u32
            } else if b0 & 0xc0 == 0x80 {
                // 2-byte length: 0x80..0xBF
                if i >= data.len() {
                    break;
                }
                let b1 = data[i];
                i += 1;
                ((b0 as u32 & 0x7f) << 8) | b1 as u32
            } else if b0 & 0xe0 == 0xc0 {
                // 3-byte length: 0xC0..0xDF
                if i + 1 > data.len() {
                    break;
                }
                let b1 = data[i];
                i += 1;
                let b2 = data[i];
                i += 1;
                ((b0 as u32 & 0x3f) << 16) | ((b1 as u32) << 8) | b2 as u32
            } else {
                // 4-byte length: 0xE0..0xFF
                if i + 2 > data.len() {
                    break;
                }
                let b1 = data[i];
                i += 1;
                let b2 = data[i];
                i += 1;
                let b3 = data[i];
                i += 1;
                ((b0 as u32 & 0x1f) << 24) | ((b1 as u32) << 16) | ((b2 as u32) << 8) | b3 as u32
            }
        };

        let remaining = expected_pixels - pixels.len();
        let fill = (run_len as usize).min(remaining);
        for _ in 0..fill {
            pixels.push(pixel);
        }
    }

    pixels.resize(expected_pixels, 0);
    pixels
}

/// Encodes a flat grayscale pixel buffer as an 8-bit grayscale PNG.
fn encode_pixels_as_grayscale_png(
    width: u32,
    height: u32,
    pixels: &[u8],
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut encoder = png::Encoder::new(&mut out, width, height);
    encoder.set_color(png::ColorType::Grayscale);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder
        .write_header()
        .map_err(|e| format!("CTB PNG header write failed: {e}"))?;
    writer
        .write_image_data(pixels)
        .map_err(|e| format!("CTB PNG data write failed: {e}"))?;
    drop(writer);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::ctb_metadata::parse_timing_model_from_metadata;
    use super::{
        build_ctb_container_bytes, ctb_layer_rle_xor, ctb_preview,
        decode_embedded_disclaimer_bytes, normalize_to_binary_mask, parse_ctb_build_model_from_job,
        parse_threshold_from_metadata, push_ctb_run, rle_encode_mask_row_major, CtbPreparedLayer,
        CTB_DISCLAIMER_SIZE, CTB_HEADER_SIZE,
    };
    use crate::types::SliceJobV3;

    fn make_test_job() -> SliceJobV3 {
        SliceJobV3 {
            output_format: ".ctb".to_string(),
            source_width_px: 4,
            source_height_px: 4,
            width_px: 4,
            height_px: 4,
            build_width_mm: 10.0,
            build_depth_mm: 20.0,
            layer_height_mm: 0.05,
            total_layers: 2,
            anti_aliasing_level: "Off".to_string(),
            metadata_json: "{}".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn binary_mask_thresholds_values() {
        let (masked, lit) = normalize_to_binary_mask(&[0, 12, 127, 128, 255], 127);
        assert_eq!(masked, vec![0, 0, 0, 255, 255]);
        assert_eq!(lit, 2);
    }

    #[test]
    fn rle_encodes_simple_runs() {
        let encoded = rle_encode_mask_row_major(&[0, 0, 255, 255, 255, 0]);
        assert_eq!(encoded, vec![0x80, 0x02, 0xff, 0x03, 0x00]);
    }

    #[test]
    fn rle_handles_long_runs_above_u8() {
        let input = vec![255u8; 300];
        let encoded = rle_encode_mask_row_major(&input);
        assert_eq!(encoded, vec![0xff, 0x81, 0x2c]);
    }

    #[test]
    fn ctb_run_encoder_supports_4byte_span_prefix() {
        let mut out = Vec::new();
        push_ctb_run(&mut out, 0x2A_BC_DE_u32, 255);

        assert_eq!(out[0], 0xff);
        assert_eq!(out[1] & 0xE0, 0xE0);
        assert_eq!(out.len(), 5);
    }

    #[test]
    fn metadata_threshold_defaults_when_missing_or_invalid() {
        assert_eq!(parse_threshold_from_metadata("{}"), 127);
        assert_eq!(parse_threshold_from_metadata("not-json"), 127);
    }

    #[test]
    fn metadata_threshold_reads_supported_paths() {
        let direct = r#"{ "ctb": { "binaryThreshold": 180 } }"#;
        let nested = r#"{ "export": { "ctb": { "binaryThreshold": 200 } } }"#;

        assert_eq!(parse_threshold_from_metadata(direct), 180);
        assert_eq!(parse_threshold_from_metadata(nested), 200);
    }

    #[test]
    fn metadata_timing_prefers_ctb_over_material() {
        let meta = r#"{
            "material": {
                "liftDistanceMm": 4.0,
                "liftSpeedMmMin": 40.0,
                "retractSpeedMmMin": 120.0,
                "bottomLayerCount": 4
            },
            "export": {
                "ctb": {
                    "liftDistanceMm": 7.5,
                    "liftSpeedMmMin": 65.0,
                    "retractSpeedMmMin": 190.0,
                    "bottomLayerCount": 8
                }
            }
        }"#;

        let timing = parse_timing_model_from_metadata(meta);
        assert!((timing.lift_distance_mm - 7.5).abs() < f32::EPSILON);
        assert!((timing.lift_speed_mm_min - 65.0).abs() < f32::EPSILON);
        assert!((timing.retract_speed_mm_min - 190.0).abs() < f32::EPSILON);
        assert_eq!(timing.bottom_layer_count, 8);
    }

    #[test]
    fn metadata_timing_simple_mode_zeroes_two_stage_fields() {
        let meta = r#"{
            "printer": {
                "settingsMode": "simple"
            },
            "export": {
                "ctb": {
                    "liftDistanceMm": 6.0,
                    "liftSpeedMmMin": 60.0,
                    "retractDistanceMm": 4.0,
                    "retractSpeedMmMin": 150.0,
                    "liftDistance2Mm": 2.5,
                    "liftSpeed2MmMin": 75.0,
                    "retractDistance2Mm": 1.25,
                    "retractSpeed2MmMin": 110.0,
                    "bottomRetractSpeed2MmMin": 90.0,
                    "bottomRetractHeight2Mm": 0.8
                }
            }
        }"#;

        let timing = parse_timing_model_from_metadata(meta);
        assert!((timing.lift_distance2_mm - 0.0).abs() < f32::EPSILON);
        assert!((timing.lift_speed2_mm_min - 0.0).abs() < f32::EPSILON);
        assert!((timing.retract_distance2_mm - 0.0).abs() < f32::EPSILON);
        assert!((timing.retract_speed2_mm_min - 0.0).abs() < f32::EPSILON);
        assert!((timing.bottom_retract_speed2_mm_min - 0.0).abs() < f32::EPSILON);
        assert!((timing.bottom_retract_height2_mm - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn build_model_infers_per_layer_settings_for_twostage_mode() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "printer": {
                "settingsMode": "twostage"
            },
            "ctb": {
                "version": 5
            }
        }"#
        .to_string();

        let build = parse_ctb_build_model_from_job(&job);
        assert!(build.per_layer_settings);
    }

    #[test]
    fn ctb_print_parameters_store_total_lift_heights_for_uvtools_stage1_derivation() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "material": {
                "liftDistanceMm": 1.0,
                "liftSpeedMmMin": 65.0,
                "normalExposureSec": 2.0,
                "bottomExposureSec": 20.0,
                "bottomLayerCount": 2
            },
            "ctb": {
                "version": 5,
                "settingsMode": "twostage",
                "bottomLiftDistanceMm": 3.0,
                "bottomLiftDistance2Mm": 4.0,
                "bottomLiftSpeedMmMin": 60.0,
                "bottomLiftSpeed2MmMin": 180.0,
                "liftDistance2Mm": 4.0,
                "liftSpeed2MmMin": 240.0
            }
        }"#
        .to_string();

        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");

        let print_params_offset = u32::from_le_bytes(bytes[84..88].try_into().unwrap()) as usize;
        let bottom_lift_distance_total = f32::from_le_bytes(
            bytes[print_params_offset..print_params_offset + 4]
                .try_into()
                .unwrap(),
        );
        let bottom_lift_speed = f32::from_le_bytes(
            bytes[print_params_offset + 4..print_params_offset + 8]
                .try_into()
                .unwrap(),
        );
        let normal_lift_distance_total = f32::from_le_bytes(
            bytes[print_params_offset + 8..print_params_offset + 12]
                .try_into()
                .unwrap(),
        );
        let normal_lift_speed = f32::from_le_bytes(
            bytes[print_params_offset + 12..print_params_offset + 16]
                .try_into()
                .unwrap(),
        );

        // UVTools derives stage1 lift heights from (total_height - height2).
        assert!((bottom_lift_distance_total - 7.0).abs() < f32::EPSILON); // 3.0 + 4.0
        assert!((bottom_lift_speed - 60.0).abs() < f32::EPSILON);
        assert!((normal_lift_distance_total - 5.0).abs() < f32::EPSILON); // 1.0 + 4.0
        assert!((normal_lift_speed - 65.0).abs() < f32::EPSILON);

        let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;
        assert_eq!(bytes[slicer_offset + 39], 0x50);
    }

    #[test]
    fn ctb_slicer_info_stores_two_stage_lift_values() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "material": {
                "liftDistanceMm": 1.0,
                "liftSpeedMmMin": 65.0,
                "normalExposureSec": 2.0,
                "bottomExposureSec": 20.0,
                "bottomLayerCount": 2,
                "retractDistance2Mm": 1.0,
                "retractSpeed2MmMin": 60.0
            },
            "ctb": {
                "version": 5,
                "settingsMode": "twostage",
                "bottomLiftDistance2Mm": 4.0,
                "bottomLiftSpeed2MmMin": 180.0,
                "liftDistance2Mm": 4.0,
                "liftSpeed2MmMin": 240.0,
                "waitTimeAfterLiftSec": 0.75
            }
        }"#
        .to_string();

        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;

        let bottom_lift_height2 =
            f32::from_le_bytes(bytes[slicer_offset..slicer_offset + 4].try_into().unwrap());
        let bottom_lift_speed2 = f32::from_le_bytes(
            bytes[slicer_offset + 4..slicer_offset + 8]
                .try_into()
                .unwrap(),
        );
        let lift_height2 = f32::from_le_bytes(
            bytes[slicer_offset + 8..slicer_offset + 12]
                .try_into()
                .unwrap(),
        );
        let lift_speed2 = f32::from_le_bytes(
            bytes[slicer_offset + 12..slicer_offset + 16]
                .try_into()
                .unwrap(),
        );
        let retract_height2 = f32::from_le_bytes(
            bytes[slicer_offset + 16..slicer_offset + 20]
                .try_into()
                .unwrap(),
        );
        let retract_speed2 = f32::from_le_bytes(
            bytes[slicer_offset + 20..slicer_offset + 24]
                .try_into()
                .unwrap(),
        );
        let wait_after_lift = f32::from_le_bytes(
            bytes[slicer_offset + 24..slicer_offset + 28]
                .try_into()
                .unwrap(),
        );

        assert!((bottom_lift_height2 - 4.0).abs() < f32::EPSILON);
        assert!((bottom_lift_speed2 - 180.0).abs() < f32::EPSILON);
        assert!((lift_height2 - 4.0).abs() < f32::EPSILON);
        assert!((lift_speed2 - 240.0).abs() < f32::EPSILON);
        assert!((retract_height2 - 1.0).abs() < f32::EPSILON);
        assert!((retract_speed2 - 60.0).abs() < f32::EPSILON);
        assert!((wait_after_lift - 0.75).abs() < f32::EPSILON);
    }

    #[test]
    fn layer_xor_roundtrip_restores_original() {
        let seed = 0x1234_5678;
        let layer_index = 7;
        let original = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 255];

        let mut encrypted = original.clone();
        ctb_layer_rle_xor(seed, layer_index, &mut encrypted);
        assert_ne!(encrypted, original);

        ctb_layer_rle_xor(seed, layer_index, &mut encrypted);
        assert_eq!(encrypted, original);
    }

    #[test]
    fn preview_rle_encodes_non_empty() {
        let rgb = vec![255u8, 0, 0, 255, 0, 0, 0, 0, 255];
        let rle = ctb_preview::encode_preview_rgb15_rle(&rgb);
        assert!(!rle.is_empty());
    }

    #[test]
    fn data_url_or_plain_base64_decode_works() {
        let tiny_png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9WlH0wAAAABJRU5ErkJggg==";
        let data_url = format!("data:image/png;base64,{tiny_png}");

        let a =
            ctb_preview::decode_base64_data_url_or_plain(tiny_png).expect("plain should decode");
        let b = ctb_preview::decode_base64_data_url_or_plain(&data_url)
            .expect("data-url should decode");
        assert_eq!(a, b);
    }

    #[test]
    fn embedded_disclaimer_decodes_without_plaintext_constant() {
        let bytes = decode_embedded_disclaimer_bytes().expect("embedded disclaimer should decode");
        assert!(!bytes.is_empty());
        assert!(bytes.len() <= CTB_DISCLAIMER_SIZE);
    }

    #[test]
    fn ctb_container_writes_magic_and_header_fields() {
        let mut job = make_test_job();
        job.metadata_json = r#"{ "ctb": { "version": 4 } }"#.to_string();
        let prepared = vec![
            CtbPreparedLayer {
                index: 0,
                source_len: 16,
                encoded: vec![2, 0, 255],
            },
            CtbPreparedLayer {
                index: 1,
                source_len: 16,
                encoded: vec![1, 0, 0],
            },
        ];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        assert!(bytes.len() > CTB_HEADER_SIZE as usize);

        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let width = u32::from_le_bytes(bytes[52..56].try_into().unwrap());
        let height = u32::from_le_bytes(bytes[56..60].try_into().unwrap());
        let layers = u32::from_le_bytes(bytes[68..72].try_into().unwrap());

        assert_eq!(magic, 0x12FD_0106);
        assert_eq!(version, 4);
        assert_eq!(width, 4);
        assert_eq!(height, 4);
        assert_eq!(layers, 2);
    }

    #[test]
    fn ctb_container_transfers_machine_name_bedsizez_and_modified_date() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "ctb": {
                "version": 4,
                "MachineName": "DF Mega Printer",
                "ModifiedDate": 1735689600
            },
            "printer": {
                "buildVolumeMm": {
                    "width": 10,
                    "depth": 20,
                    "height": 180
                }
            }
        }"#
        .to_string();

        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");

        let bed_size_z = f32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let modified_date = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        assert!((bed_size_z - 180.0).abs() < 0.001);
        assert_eq!(modified_date, 1_735_689_600);

        let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;
        let machine_name_offset = u32::from_le_bytes(
            bytes[slicer_offset + 28..slicer_offset + 32]
                .try_into()
                .unwrap(),
        ) as usize;
        let machine_name_size = u32::from_le_bytes(
            bytes[slicer_offset + 32..slicer_offset + 36]
                .try_into()
                .unwrap(),
        ) as usize;
        let machine_name = String::from_utf8_lossy(
            &bytes[machine_name_offset..machine_name_offset.saturating_add(machine_name_size)],
        );
        assert_eq!(machine_name, "DF Mega Printer");
    }

    #[test]
    fn ctb_container_embeds_preview_offsets() {
        let job = make_test_job();
        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        let large_preview_offset = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
        let small_preview_offset = u32::from_le_bytes(bytes[72..76].try_into().unwrap());

        assert!(large_preview_offset >= CTB_HEADER_SIZE);
        assert!(small_preview_offset > large_preview_offset);
    }

    #[test]
    fn ctb_v4_writes_disclaimer_and_print_params_v4() {
        let mut job = make_test_job();
        job.metadata_json = r#"{ "ctb": { "version": 4 } }"#.to_string();
        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");

        let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;
        let print_params_v4_addr = u32::from_le_bytes(
            bytes[slicer_offset + 64..slicer_offset + 68]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(print_params_v4_addr > slicer_offset);

        let disclaimer_addr = u32::from_le_bytes(
            bytes[print_params_v4_addr + 72..print_params_v4_addr + 76]
                .try_into()
                .unwrap(),
        ) as usize;
        let disclaimer_len = u32::from_le_bytes(
            bytes[print_params_v4_addr + 76..print_params_v4_addr + 80]
                .try_into()
                .unwrap(),
        ) as usize;

        assert_eq!(disclaimer_len, CTB_DISCLAIMER_SIZE);
        assert!(disclaimer_addr + disclaimer_len <= bytes.len());
    }

    #[test]
    fn ctb_v5_writes_resin_parameters_address() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "ctb": {
                "version": 5,
                "resinName": "FastResin",
                "resinType": "ABS-Like",
                "ModifiedDate": 1735689600
            }
        }"#
        .to_string();

        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");

        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let encryption_key = u32::from_le_bytes(bytes[100..104].try_into().unwrap());
        assert_eq!(magic, 0x12FD_0106);
        assert_eq!(version, 5);
        assert_ne!(encryption_key, 0);

        let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;
        let modified_timestamp_minutes = u32::from_le_bytes(
            bytes[slicer_offset + 40..slicer_offset + 44]
                .try_into()
                .unwrap(),
        );
        let print_params_v4_addr = u32::from_le_bytes(
            bytes[slicer_offset + 64..slicer_offset + 68]
                .try_into()
                .unwrap(),
        ) as usize;
        let resin_addr = u32::from_le_bytes(
            bytes[print_params_v4_addr + 80..print_params_v4_addr + 84]
                .try_into()
                .unwrap(),
        ) as usize;

        assert!(resin_addr > print_params_v4_addr);
        assert!(resin_addr < bytes.len());
        assert_eq!(modified_timestamp_minutes, 1_735_689_600 / 60);
    }

    #[test]
    fn ctb_v5_default_encryption_key_is_stable_and_nonzero() {
        let mut job = make_test_job();
        job.metadata_json = r#"{ "ctb": { "version": 5 } }"#.to_string();

        let first = parse_ctb_build_model_from_job(&job);
        let second = parse_ctb_build_model_from_job(&job);

        assert_eq!(first.layer_xor_key, 0xEFBE_ADDE);
        assert_eq!(second.layer_xor_key, 0xEFBE_ADDE);
        assert_ne!(first.layer_xor_key, 0);
        assert_eq!(first.layer_xor_key, second.layer_xor_key);
    }

    #[test]
    fn ctb_v5enc_emits_encrypted_magic_and_signature_header_fields() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "ctb": {
                "version": 5,
                "aes": {
                    "enabled": true
                }
            }
        }"#
        .to_string();
        job.format_version = Some("v5enc".to_string());

        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("v5enc mode should build");

        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(magic, 0x12FD_0107);

        let settings_size = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let settings_offset = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let signature_size = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let signature_offset = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        assert_eq!(settings_size, 288);
        assert_eq!(settings_offset, 48);
        assert_eq!(signature_size, 32);
        assert!(signature_offset > 48 + 288);

        let trailer = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        assert_eq!(trailer, 1_833_054_899);
    }

    #[test]
    fn ctb_v5enc_settings_store_two_stage_lift_values() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "material": {
                "liftDistanceMm": 1.0,
                "liftSpeedMmMin": 65.0,
                "normalExposureSec": 2.0,
                "bottomExposureSec": 20.0,
                "bottomLayerCount": 2,
                "retractDistance2Mm": 1.0,
                "retractSpeed2MmMin": 60.0
            },
            "ctb": {
                "version": 5,
                "settingsMode": "twostage",
                "bottomLiftDistance2Mm": 4.0,
                "bottomLiftSpeed2MmMin": 180.0,
                "liftDistance2Mm": 4.0,
                "liftSpeed2MmMin": 240.0,
                "waitTimeAfterLiftSec": 0.75
            }
        }"#
        .to_string();
        job.format_version = Some("v5enc".to_string());

        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        let bytes = build_ctb_container_bytes(&job, &prepared).expect("v5enc mode should build");
        let mut settings = bytes[super::ctb_types::CTB_ENCRYPTED_SETTINGS_OFFSET as usize
            ..(super::ctb_types::CTB_ENCRYPTED_SETTINGS_OFFSET
                + super::ctb_types::CTB_ENCRYPTED_SETTINGS_SIZE) as usize]
            .to_vec();
        let (key, iv) = super::ctb_crypto::ctb_default_key_iv();
        super::ctb_crypto::ctb_decrypt_in_place_no_padding(&mut settings, &key, &iv)
            .expect("settings block should decrypt");

        let read_f32 = |offset: usize| -> f32 {
            f32::from_le_bytes(settings[offset..offset + 4].try_into().unwrap())
        };

        // Global print-parameter lift heights store TOTAL heights (stage1 + stage2).
        assert!((read_f32(84) - 4.0).abs() < f32::EPSILON); // bottom total = 0 + 4 (no bottom stage1 provided)
        assert!((read_f32(92) - 5.0).abs() < f32::EPSILON); // normal total = 1 + 4

        // SlicerSettings fields 33..39 in UVTools mapping.
        assert!((read_f32(132) - 4.0).abs() < f32::EPSILON); // BottomLiftHeight2
        assert!((read_f32(136) - 180.0).abs() < f32::EPSILON); // BottomLiftSpeed2
        assert!((read_f32(140) - 4.0).abs() < f32::EPSILON); // LiftHeight2
        assert!((read_f32(144) - 240.0).abs() < f32::EPSILON); // LiftSpeed2
        assert!((read_f32(148) - 1.0).abs() < f32::EPSILON); // RetractHeight2
        assert!((read_f32(152) - 60.0).abs() < f32::EPSILON); // RetractSpeed2
        assert!((read_f32(156) - 0.75).abs() < f32::EPSILON); // RestTimeAfterLift
    }

    #[test]
    fn ctb_v5enc_ignores_legacy_invalid_aes_key_or_iv_metadata() {
        let mut job = make_test_job();
        job.metadata_json = r#"{
            "ctb": {
                "version": 5,
                "aes": {
                    "enabled": true,
                    "keyBase64": "MDEyMzQ1Njc4OWFiY2RlZg==",
                    "ivBase64": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NQ=="
                }
            }
        }"#
        .to_string();

        let prepared = vec![CtbPreparedLayer {
            index: 0,
            source_len: 16,
            encoded: vec![2, 0, 255],
        }];

        job.format_version = Some("v5enc".to_string());
        let bytes = build_ctb_container_bytes(&job, &prepared)
            .expect("invalid legacy aes fields are ignored");
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(magic, 0x12FD_0107);
    }

    // ---- Wait-before-cure profile -------------------------------------------------

    /// Reads per-layer "wait before cure" values back out of an encoded CTB v4/v5 file,
    /// the same way UVTools / the Whale3 CTB Wait Editor locate them.
    fn read_wait_before_cure_per_layer(bytes: &[u8]) -> Vec<f32> {
        let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let f32_at = |o: usize| f32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let layer_defs = u32_at(0x40) as usize;
        let layer_count = u32_at(0x44) as usize;
        (0..layer_count)
            .map(|i| {
                let o = layer_defs + 36 * i;
                let data_addr = u32_at(o + 12) as u64;
                let page = u32_at(o + 20) as u64;
                let ex = (page * 4_294_967_296 + data_addr - 84) as usize;
                // Extended block starts with a copy of the 36-byte table entry.
                assert_eq!(&bytes[o..o + 36], &bytes[ex..ex + 36], "layer {i} extended block mismatch");
                f32_at(ex + 76)
            })
            .collect()
    }

    fn wait_profile_job(version: u32, layers: usize, extra: &str) -> (SliceJobV3, Vec<CtbPreparedLayer>) {
        let mut job = make_test_job();
        job.total_layers = layers as u32;
        job.metadata_json = format!(
            r#"{{
                "printer": {{ "settingsMode": "simple" }},
                "material": {{ "bottomLayerCount": 3, "bottomExposureSec": 30, "normalExposureSec": 2 }},
                "ctb": {{
                    "version": {version},
                    "waitTimeBeforeCureSec": 2,
                    "bottomWaitTimeBeforeCureSec": 20,
                    "waitProfileEnabled": true,
                    "waitProfileLayer1Sec": 600,
                    "waitProfileLayer2Sec": 300,
                    "waitProfileHoldHeightMm": 0.25,
                    "waitProfileTransitionMethod": "time",
                    "waitProfileDecreasePerLayerSec": 5
                    {extra}
                }}
            }}"#
        );
        let prepared = (0..layers)
            .map(|index| CtbPreparedLayer { index, source_len: 16, encoded: vec![2, 0, 255] })
            .collect();
        (job, prepared)
    }

    #[test]
    fn wait_profile_is_written_per_layer_in_ctb_v4_and_v5() {
        for version in [4_u32, 5] {
            let (job, prepared) = wait_profile_job(version, 12, "");
            let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
            let waits = read_wait_before_cure_per_layer(&bytes);
            // L1, L2, bottom (layer 3), held to 0.25 mm (layers 4-5), then -5 s/layer to 2 s.
            assert_eq!(
                waits,
                vec![600.0, 300.0, 20.0, 20.0, 20.0, 15.0, 10.0, 5.0, 2.0, 2.0, 2.0, 2.0],
                "CTB v{version}"
            );

            // Per-layer settings flag must be on, even in simple mode.
            let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;
            assert_ne!(bytes[slicer_offset + 39], 0, "CTB v{version} per-layer flag");
        }
    }

    #[test]
    fn wait_profile_distance_transition_is_written() {
        let (job, prepared) = wait_profile_job(
            4,
            10,
            r#", "waitProfileTransitionMethod": "distance", "waitProfileTransitionDistanceMm": 0.1"#,
        );
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        // 20 -> 2 over 0.1 mm = 2 layers of 9 s.
        assert_eq!(
            read_wait_before_cure_per_layer(&bytes),
            vec![600.0, 300.0, 20.0, 20.0, 20.0, 11.0, 2.0, 2.0, 2.0, 2.0]
        );
    }

    #[test]
    fn wait_profile_disabled_keeps_original_output() {
        let (job, prepared) = wait_profile_job(6, 6, r#", "waitProfileEnabled": false"#);
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        // Schedule off: the plain two-tier rule, using the same bottom and normal waits.
        // No waitTimeBottomLayerCount in the metadata, so it falls back to the 3 bottom layers.
        assert_eq!(read_wait_before_cure_per_layer(&bytes), vec![20.0, 20.0, 20.0, 2.0, 2.0, 2.0]);
    }

    #[test]
    fn wait_profile_overrides_explicit_per_layer_settings_false() {
        let (mut job, _) = wait_profile_job(4, 4, r#", "perLayerSettings": false"#);
        assert!(parse_ctb_build_model_from_job(&job).per_layer_settings);
        job.metadata_json = job.metadata_json.replace(r#""waitProfileEnabled": true"#, r#""waitProfileEnabled": false"#);
        assert!(!parse_ctb_build_model_from_job(&job).per_layer_settings);
    }

    #[test]
    fn print_time_estimate_includes_profile_waits() {
        let (job, prepared) = wait_profile_job(4, 12, "");
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        let print_time = u32::from_le_bytes(bytes[0x4c..0x50].try_into().unwrap());
        let waits: f32 = read_wait_before_cure_per_layer(&bytes).iter().sum();
        // 3 bottom layers x 30 s + 9 normal layers x 2 s of exposure, plus every wait,
        // plus the fixed per-layer firmware overhead.
        let overhead = 12.0 * super::ctb_types::CTB_PER_LAYER_OVERHEAD_SEC;
        let expected = (3.0 * 30.0 + 9.0 * 2.0 + waits + overhead).round() as u32;
        assert_eq!(print_time, expected);
    }

    #[test]
    fn wait_profile_metadata_parsing() {
        let meta = r#"{
            "printer": { "settingsMode": "simple" },
            "export": { "ctb": {
                "waitProfileEnabled": true,
                "waitProfileLayer1Sec": 0,
                "waitProfileLayer2Sec": 45.5,
                "bottomWaitTimeBeforeCureSec": 30,
                "waitProfileHoldHeightMm": 2,
                "waitProfileTransitionMethod": "Distance",
                "waitProfileTransitionDistanceMm": 1.5
            } }
        }"#;
        let timing = parse_timing_model_from_metadata(meta);
        assert_eq!(timing.bottom_wait_time_before_cure_sec, 30.0);
        let p = timing.wait_profile.expect("profile should parse");
        assert_eq!(p.layer1_wait_sec, None); // 0 = same as bottom wait
        assert_eq!(p.layer2_wait_sec, Some(45.5));
        assert_eq!(p.hold_height_mm, 2.0);
        assert_eq!(
            p.transition,
            super::ctb_types::CtbWaitTransition::Distance { distance_mm: 1.5 }
        );
        assert!(parse_timing_model_from_metadata("{}").wait_profile.is_none());
    }

    // ---- Bottom wait times in simple mode -----------------------------------------

    /// Reads per-layer wait before cure / after cure / after lift out of an encoded file.
    fn read_all_waits_per_layer(bytes: &[u8]) -> Vec<(f32, f32, f32)> {
        let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let f32_at = |o: usize| f32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let layer_defs = u32_at(0x40) as usize;
        (0..u32_at(0x44) as usize)
            .map(|i| {
                let o = layer_defs + 36 * i;
                let ex = ((u32_at(o + 20) as u64) * 4_294_967_296 + u32_at(o + 12) as u64 - 84) as usize;
                // Extended block order: wait after cure, after lift, then before cure.
                (f32_at(ex + 76), f32_at(ex + 68), f32_at(ex + 72))
            })
            .collect()
    }

    fn bottom_wait_job(layers: usize) -> (SliceJobV3, Vec<CtbPreparedLayer>) {
        let mut job = make_test_job();
        job.total_layers = layers as u32;
        job.metadata_json = r#"{
            "printer": { "settingsMode": "simple" },
            "material": { "bottomLayerCount": 2, "waitTimeBottomLayerCount": 4 },
            "ctb": {
                "version": 4,
                "waitTimeBeforeCureSec": 1,
                "waitTimeAfterCureSec": 0.5,
                "waitTimeAfterLiftSec": 0.25,
                "bottomWaitTimeBeforeCureSec": 20,
                "bottomWaitTimeAfterCureSec": 8,
                "bottomWaitTimeAfterLiftSec": 4
            }
        }"#
        .to_string();
        let prepared = (0..layers)
            .map(|index| CtbPreparedLayer { index, source_len: 16, encoded: vec![2, 0, 255] })
            .collect();
        (job, prepared)
    }

    #[test]
    fn simple_mode_keeps_separate_bottom_wait_times() {
        let (job, prepared) = bottom_wait_job(6);
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        let bottom = (20.0, 8.0, 4.0);
        let normal = (1.0, 0.5, 0.25);
        // waitTimeBottomLayerCount = 4, so layers 0-3 are bottom waits.
        assert_eq!(
            read_all_waits_per_layer(&bytes),
            vec![bottom, bottom, bottom, bottom, normal, normal]
        );
    }

    #[test]
    fn simple_mode_enables_per_layer_settings_for_bottom_waits() {
        let (job, _) = bottom_wait_job(4);
        assert!(parse_ctb_build_model_from_job(&job).per_layer_settings);

        // Without bottom waits, simple mode stays as it was.
        let mut plain = job.clone();
        plain.metadata_json = r#"{"printer":{"settingsMode":"simple"},"ctb":{"version":4}}"#.to_string();
        assert!(!parse_ctb_build_model_from_job(&plain).per_layer_settings);
    }

    #[test]
    fn simple_mode_still_simplifies_two_stage_motion() {
        let job = SliceJobV3 {
            metadata_json: r#"{"printer":{"settingsMode":"simple"},
                "ctb":{"liftDistance2Mm":3,"liftSpeed2MmMin":90,"bottomWaitTimeAfterCureSec":8}}"#
                .to_string(),
            ..make_test_job()
        };
        let timing = parse_timing_model_from_metadata(&job.metadata_json);
        assert_eq!(timing.lift_distance2_mm, 0.0);
        assert_eq!(timing.lift_speed2_mm_min, 0.0);
        assert_eq!(timing.bottom_wait_time_after_cure_sec, 8.0);
    }

    #[test]
    fn bottom_layers_get_their_own_projector_pwm() {
        let mut job = make_test_job();
        job.total_layers = 5;
        job.metadata_json = r#"{
            "printer": { "settingsMode": "simple" },
            "material": { "bottomLayerCount": 2 },
            "ctb": { "version": 4, "projectorPwmPercent": 100, "bottomProjectorPwmPercent": 60 }
        }"#
        .to_string();
        let prepared: Vec<CtbPreparedLayer> = (0..5)
            .map(|index| CtbPreparedLayer { index, source_len: 16, encoded: vec![2, 0, 255] })
            .collect();
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");

        let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let layer_defs = u32_at(0x40) as usize;
        let per_layer_pwm: Vec<f32> = (0..5)
            .map(|i| {
                let o = layer_defs + 36 * i;
                let ex = ((u32_at(o + 20) as u64) * 4_294_967_296 + u32_at(o + 12) as u64 - 84) as usize;
                f32::from_le_bytes(bytes[ex + 80..ex + 84].try_into().unwrap())
            })
            .collect();
        // 60% -> 153, 100% -> 255.
        assert_eq!(per_layer_pwm, vec![153.0, 153.0, 255.0, 255.0, 255.0]);

        // Header carries both values too (normal first, then bottom).
        assert_eq!(u16::from_le_bytes(bytes[96..98].try_into().unwrap()), 255);
        assert_eq!(u16::from_le_bytes(bytes[98..100].try_into().unwrap()), 153);
    }

    #[test]
    fn all_three_waits_switch_over_at_the_same_layer_with_the_schedule_on() {
        let mut job = make_test_job();
        job.total_layers = 8;
        job.metadata_json = r#"{
            "printer": { "settingsMode": "simple" },
            "material": { "bottomLayerCount": 2, "waitTimeBottomLayerCount": 4 },
            "ctb": {
                "version": 4,
                "bottomWaitTimeBeforeCureSec": 30, "waitTimeBeforeCureSec": 3,
                "bottomWaitTimeAfterCureSec": 8, "waitTimeAfterCureSec": 1,
                "bottomWaitTimeAfterLiftSec": 4, "waitTimeAfterLiftSec": 0.5,
                "waitProfileEnabled": true,
                "waitProfileLayer1Sec": 600,
                "waitProfileLayer2Sec": 300,
                "waitProfileHoldHeightMm": 0,
                "waitProfileDecreasePerLayerSec": 9
            }
        }"#
        .to_string();
        let prepared: Vec<CtbPreparedLayer> = (0..8)
            .map(|index| CtbPreparedLayer { index, source_len: 16, encoded: vec![2, 0, 255] })
            .collect();
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");

        let waits = read_all_waits_per_layer(&bytes);
        // Before cure: layer 1 and 2 overrides, then bottom up to Bottom Wait Layers (4), then normal.
        assert_eq!(
            waits.iter().map(|w| w.0).collect::<Vec<_>>(),
            vec![600.0, 300.0, 30.0, 30.0, 3.0, 3.0, 3.0, 3.0]
        );
        // After cure / after lift: plain bottom vs normal at the same layer.
        assert_eq!(
            waits.iter().map(|w| w.1).collect::<Vec<_>>(),
            vec![8.0, 8.0, 8.0, 8.0, 1.0, 1.0, 1.0, 1.0]
        );
        assert_eq!(
            waits.iter().map(|w| w.2).collect::<Vec<_>>(),
            vec![4.0, 4.0, 4.0, 4.0, 0.5, 0.5, 0.5, 0.5]
        );
    }

    // ---- Whale3 first-layer movement ------------------------------------------

    /// (position z, lift total, lift speed, lift2, retract speed, retract2, retract speed2) per layer.
    fn read_motion_per_layer(bytes: &[u8]) -> Vec<(f32, f32, f32, f32, f32, f32, f32)> {
        let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let f32_at = |o: usize| f32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let layer_defs = u32_at(0x40) as usize;
        (0..u32_at(0x44) as usize)
            .map(|i| {
                let o = layer_defs + 36 * i;
                let ex = ((u32_at(o + 20) as u64) * 4_294_967_296 + u32_at(o + 12) as u64 - 84) as usize;
                assert_eq!(&bytes[o..o + 36], &bytes[ex..ex + 36], "layer {i} extended block mismatch");
                (
                    f32_at(o),
                    f32_at(ex + 40),
                    f32_at(ex + 44),
                    f32_at(ex + 48),
                    f32_at(ex + 56),
                    f32_at(ex + 60),
                    f32_at(ex + 64),
                )
            })
            .collect()
    }

    fn first_layer_job(version: u32, mode: &str, extra: &str) -> (SliceJobV3, Vec<CtbPreparedLayer>) {
        let mut job = make_test_job();
        job.total_layers = 4;
        job.metadata_json = format!(
            r#"{{
                "printer": {{ "settingsMode": "simple" }},
                "material": {{
                    "bottomLayerCount": 2, "bottomExposureSec": 30, "normalExposureSec": 2,
                    "bottomLiftDistanceMm": 6, "bottomLiftSpeedMmMin": 60, "bottomRetractSpeedMmMin": 90,
                    "liftDistanceMm": 5, "liftSpeedMmMin": 120, "retractSpeedMmMin": 150
                }},
                "ctb": {{ "version": {version}, "firstLayerMoveMode": "{mode}" {extra} }}
            }}"#
        );
        let prepared = (0..4)
            .map(|index| CtbPreparedLayer { index, source_len: 16, encoded: vec![2, 0, 255] })
            .collect();
        (job, prepared)
    }

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-5
    }

    #[test]
    fn first_layer_default_mode_is_unchanged() {
        let (job, prepared) = first_layer_job(4, "default", "");
        let m = read_motion_per_layer(&build_ctb_container_bytes(&job, &prepared).unwrap());
        let z: Vec<f32> = m.iter().map(|l| l.0).collect();
        assert!(z.iter().zip([0.05, 0.1, 0.15, 0.2]).all(|(a, b)| close(*a, b)), "{z:?}");
        assert!(close(m[0].1, 6.0) && close(m[0].2, 60.0) && close(m[0].4, 90.0));
        assert!(!parse_ctb_build_model_from_job(&job).per_layer_settings);
    }

    #[test]
    fn first_layer_home_mode_lifts_2_5_mm_and_cures_at_home() {
        for version in [4_u32, 5] {
            let (job, prepared) = first_layer_job(version, "home", "");
            let bytes = build_ctb_container_bytes(&job, &prepared).unwrap();
            let m = read_motion_per_layer(&bytes);
            let z: Vec<f32> = m.iter().map(|l| l.0).collect();
            assert!(z.iter().zip([0.0, 0.05, 0.1, 0.15]).all(|(a, b)| close(*a, b)), "v{version} {z:?}");
            // Layer 1: one 2.5 mm stage at 20 mm/min each way.
            let l1 = m[0];
            assert!(close(l1.1, 2.5) && close(l1.2, 20.0) && close(l1.3, 0.0), "v{version} {l1:?}");
            assert!(close(l1.4, 20.0) && close(l1.5, 0.0) && close(l1.6, 20.0), "v{version} {l1:?}");
            // Layer 2 back on the bottom values, layer 3 on the normal ones.
            assert!(close(m[1].1, 6.0) && close(m[1].2, 60.0) && close(m[1].4, 90.0));
            assert!(close(m[2].1, 5.0) && close(m[2].2, 120.0) && close(m[2].4, 150.0));
            // Per-layer settings switched on so the printer reads layer 1's values.
            let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;
            assert_ne!(bytes[slicer_offset + 39], 0, "v{version} per-layer flag");
        }
    }

    #[test]
    fn first_layer_below_home_mode_cures_one_layer_below_home() {
        let (job, prepared) = first_layer_job(5, "below-home", "");
        let m = read_motion_per_layer(&build_ctb_container_bytes(&job, &prepared).unwrap());
        let z: Vec<f32> = m.iter().map(|l| l.0).collect();
        assert!(z.iter().zip([-0.05, 0.0, 0.05, 0.1]).all(|(a, b)| close(*a, b)), "{z:?}");
        assert!(close(m[0].1, 2.5) && close(m[0].2, 20.0) && close(m[0].4, 20.0));
    }

    #[test]
    fn first_layer_mode_overrides_explicit_per_layer_settings_false() {
        let (job, _) = first_layer_job(4, "home", r#", "perLayerSettings": false"#);
        assert!(parse_ctb_build_model_from_job(&job).per_layer_settings);
    }

    #[test]
    fn first_layer_print_time_uses_the_slow_move() {
        let time = |mode: &str| {
            let (job, prepared) = first_layer_job(4, mode, "");
            let bytes = build_ctb_container_bytes(&job, &prepared).unwrap();
            u32::from_le_bytes(bytes[0x4c..0x50].try_into().unwrap()) as i64
        };
        // Default layer 1: 6 mm up at 60 mm/min + 6 mm down at 90 mm/min = 10 s.
        // Home: 2.5 mm up and 2.5 mm down at 20 mm/min = 15 s. Below home: 2.5 + 2.55 mm = 15.15 s.
        assert_eq!(time("home") - time("default"), 5);
        assert_eq!(time("below-home") - time("default"), 5);
    }

    #[test]
    fn first_layer_mode_is_written_in_encrypted_v5() {
        let (mut job, prepared) = first_layer_job(5, "below-home", "");
        job.format_version = Some("v5enc".to_string());
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("v5enc should build");
        // Encrypted layer definitions start with their size (88) followed by the Z position.
        let mut zs = Vec::new();
        for o in 0..bytes.len().saturating_sub(48) {
            if u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()) == 88
                && u32::from_le_bytes(bytes[o + 24..o + 28].try_into().unwrap()) == 3
            {
                zs.push((
                    f32::from_le_bytes(bytes[o + 4..o + 8].try_into().unwrap()),
                    f32::from_le_bytes(bytes[o + 40..o + 44].try_into().unwrap()),
                ));
            }
        }
        assert_eq!(zs.len(), 4, "{zs:?}");
        assert!(close(zs[0].0, -0.05) && close(zs[0].1, 2.5), "{zs:?}");
        assert!(close(zs[1].0, 0.0) && close(zs[1].1, 6.0), "{zs:?}");
    }

    // ---- UVTools-style empty first layer --------------------------------------

    /// Per-layer view used by the empty-first-layer tests:
    /// (z, exposure, light off, lift total, lift speed, retract speed, pwm, data len).
    fn read_layers_for_empty_tests(bytes: &[u8]) -> Vec<(f32, f32, f32, f32, f32, f32, f32, u32)> {
        let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let f32_at = |o: usize| f32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let layer_defs = u32_at(0x40) as usize;
        (0..u32_at(0x44) as usize)
            .map(|i| {
                let o = layer_defs + 36 * i;
                let ex = ((u32_at(o + 20) as u64) * 4_294_967_296 + u32_at(o + 12) as u64 - 84) as usize;
                assert_eq!(&bytes[o..o + 36], &bytes[ex..ex + 36], "layer {i} extended block mismatch");
                (
                    f32_at(o),
                    f32_at(o + 4),
                    f32_at(o + 8),
                    f32_at(ex + 40),
                    f32_at(ex + 44),
                    f32_at(ex + 56),
                    f32_at(ex + 80),
                    u32_at(o + 16),
                )
            })
            .collect()
    }

    fn empty_first_layer_job(version: u32, layers: usize, extra: &str) -> (SliceJobV3, Vec<CtbPreparedLayer>) {
        let mut job = make_test_job();
        job.total_layers = layers as u32;
        job.metadata_json = format!(
            r#"{{
                "printer": {{ "settingsMode": "simple" }},
                "material": {{
                    "bottomLayerCount": 2, "bottomExposureSec": 30, "normalExposureSec": 2,
                    "bottomLiftDistanceMm": 6, "bottomLiftSpeedMmMin": 60, "bottomRetractSpeedMmMin": 90,
                    "liftDistanceMm": 5, "liftSpeedMmMin": 120, "retractSpeedMmMin": 150
                }},
                "ctb": {{
                    "version": {version},
                    "emptyFirstLayer": true,
                    "bottomWaitTimeBeforeCureSec": 120, "waitTimeBeforeCureSec": 3,
                    "bottomWaitTimeAfterCureSec": 4, "waitTimeAfterCureSec": 1,
                    "bottomWaitTimeAfterLiftSec": 3, "waitTimeAfterLiftSec": 0.5
                    {extra}
                }}
            }}"#
        );
        let prepared = (0..layers)
            .map(|index| CtbPreparedLayer { index, source_len: 16, encoded: vec![2, 0, 255] })
            .collect();
        (job, prepared)
    }

    #[test]
    fn empty_first_layer_matches_the_uvtools_shape() {
        let (job, prepared) = empty_first_layer_job(4, 4, "");
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("container should build");
        let layers = read_layers_for_empty_tests(&bytes);

        // One layer more than the model, and the blank one shares layer 1's Z.
        assert_eq!(layers.len(), 5);
        assert!(close(layers[0].0, 0.05) && close(layers[1].0, 0.05), "{:?}", (layers[0].0, layers[1].0));
        let z: Vec<f32> = layers.iter().map(|l| l.0).collect();
        assert!(z.iter().zip([0.05, 0.05, 0.1, 0.15, 0.2]).all(|(a, b)| close(*a, b)), "{z:?}");

        // The blank layer: a flash of exposure at the lowest PWM, no light-off, 0.1 mm hop
        // at the bottom speeds.
        let e = layers[0];
        assert!(close(e.1, 0.01), "exposure {:?}", e.1);
        assert!(close(e.2, 0.0), "light off {:?}", e.2);
        assert!(close(e.3, 0.1) && close(e.4, 60.0) && close(e.5, 90.0), "motion {e:?}");
        assert!(close(e.6, 1.0), "pwm {:?}", e.6);
        // ... and no waits of any kind.
        let waits = read_all_waits_per_layer(&bytes);
        assert_eq!(waits[0], (0.0, 0.0, 0.0));

        // The real layers keep their own values, starting with the real first layer.
        assert!(close(layers[1].1, 30.0) && close(layers[1].3, 6.0), "{:?}", layers[1]);
        assert_eq!(waits[1], (120.0, 4.0, 3.0));
        assert!(close(layers[3].1, 2.0) && close(layers[3].3, 5.0), "{:?}", layers[3]);

        // Header: total height unchanged, bottom layer count raised by one, per-layer on.
        let total_height = f32::from_le_bytes(bytes[28..32].try_into().unwrap());
        assert!(close(total_height, 0.2), "total height {total_height}");
        assert_eq!(u32::from_le_bytes(bytes[48..52].try_into().unwrap()), 3);
        let slicer_offset = u32::from_le_bytes(bytes[104..108].try_into().unwrap()) as usize;
        assert_ne!(bytes[slicer_offset + 39], 0, "per-layer flag");
    }

    #[test]
    fn empty_first_layer_off_changes_nothing() {
        let (on_job, prepared) = empty_first_layer_job(4, 4, "");
        let (mut off_job, _) = empty_first_layer_job(4, 4, "");
        off_job.metadata_json = off_job
            .metadata_json
            .replace(r#""emptyFirstLayer": true"#, r#""emptyFirstLayer": false"#);

        let on = build_ctb_container_bytes(&on_job, &prepared).unwrap();
        let off = build_ctb_container_bytes(&off_job, &prepared).unwrap();
        let on_layers = read_layers_for_empty_tests(&on);
        let off_layers = read_layers_for_empty_tests(&off);

        assert_eq!(off_layers.len(), 4);
        assert_eq!(u32::from_le_bytes(off[48..52].try_into().unwrap()), 2);
        // Every real layer is written exactly the same way, just one slot later.
        assert_eq!(&on_layers[1..].iter().map(|l| (l.0, l.1, l.3, l.6)).collect::<Vec<_>>(),
                   &off_layers.iter().map(|l| (l.0, l.1, l.3, l.6)).collect::<Vec<_>>());
    }

    #[test]
    fn empty_first_layer_covers_the_whole_screen_and_lights_one_pixel() {
        let (mut job, prepared) = empty_first_layer_job(4, 2, "");
        job.source_width_px = 64;
        job.source_height_px = 32;
        let bytes = build_ctb_container_bytes(&job, &prepared).unwrap();
        let layers = read_layers_for_empty_tests(&bytes);

        let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let layer_defs = u32_at(0x40) as usize;
        let addr = (u32_at(layer_defs + 20) as u64) * 4_294_967_296 + u32_at(layer_defs + 12) as u64;
        let data = &bytes[addr as usize..addr as usize + layers[0].7 as usize];

        // CTB v4 is not obfuscated, so the packets can be read straight out of the file.
        assert_eq!(u32::from_le_bytes(bytes[100..104].try_into().unwrap()), 0, "v4 xor key");
        let mut pixels = 0usize;
        let mut lit = 0usize;
        let mut i = 0usize;
        while i < data.len() {
            let code = data[i];
            i += 1;
            let value = (code & 0x7f) << 1;
            let len = if code & 0x80 == 0 {
                1
            } else {
                let n = data[i];
                i += 1;
                if n & 0x80 == 0 {
                    n as usize
                } else {
                    let v = (((n & 0x3f) as usize) << 8) | data[i] as usize;
                    i += 1;
                    v
                }
            };
            pixels += len;
            if value != 0 {
                lit += len;
            }
        }
        assert_eq!(pixels, 64 * 32, "must cover the screen exactly");
        assert_eq!(lit, 1, "exactly one lit pixel");
    }

    #[test]
    fn empty_first_layer_rekeys_obfuscated_layer_data() {
        // CTB v5 obfuscates each layer's RLE with a key derived from its file position,
        // so inserting a layer has to re-key everything behind it.
        let (job, _) = empty_first_layer_job(5, 3, "");
        let seed = parse_ctb_build_model_from_job(&job).layer_xor_key;
        assert_ne!(seed, 0, "v5 should be obfuscated");

        // Layers arrive already obfuscated for the position they were sliced at.
        let prepared: Vec<CtbPreparedLayer> = (0..3)
            .map(|index| {
                let mut encoded = vec![2_u8, 0, 255];
                ctb_layer_rle_xor(seed, index as u32, &mut encoded);
                CtbPreparedLayer { index, source_len: 16, encoded }
            })
            .collect();

        let bytes = build_ctb_container_bytes(&job, &prepared).unwrap();
        let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        assert_eq!(u32_at(100), seed, "header should advertise the same key");

        let layer_defs = u32_at(0x40) as usize;
        for file_index in 1..4u32 {
            let o = layer_defs + 36 * file_index as usize;
            let addr = ((u32_at(o + 20) as u64) * 4_294_967_296 + u32_at(o + 12) as u64) as usize;
            let len = u32_at(o + 16) as usize;
            let mut data = bytes[addr..addr + len].to_vec();
            ctb_layer_rle_xor(seed, file_index, &mut data);
            assert_eq!(data, vec![2, 0, 255], "layer at file position {file_index}");
        }
    }

    #[test]
    fn empty_first_layer_works_with_the_wait_profile_and_first_layer_movement() {
        let (job, prepared) = empty_first_layer_job(
            4,
            5,
            r#", "firstLayerMoveMode": "home",
                 "waitProfileEnabled": true,
                 "waitProfileLayer1Sec": 600, "waitProfileLayer2Sec": 300,
                 "waitProfileHoldHeightMm": 0"#,
        );
        let bytes = build_ctb_container_bytes(&job, &prepared).unwrap();
        let layers = read_layers_for_empty_tests(&bytes);
        let waits = read_all_waits_per_layer(&bytes);

        // Home mode moves the stack down one layer; the blank layer follows layer 1 there.
        let z: Vec<f32> = layers.iter().map(|l| l.0).collect();
        assert!(z.iter().zip([0.0, 0.0, 0.05, 0.1, 0.15, 0.2]).all(|(a, b)| close(*a, b)), "{z:?}");
        // It borrows that mode's slow speeds for its own hop, and still waits for nothing.
        assert!(close(layers[0].3, 0.1) && close(layers[0].4, 20.0) && close(layers[0].5, 20.0), "{:?}", layers[0]);
        assert_eq!(waits[0], (0.0, 0.0, 0.0));
        // The 600 s / 300 s layer-1 and layer-2 waits land on the real layers.
        assert_eq!(waits[1].0, 600.0);
        assert_eq!(waits[2].0, 300.0);
        // Layer 1 keeps the first-layer movement's 2.5 mm move.
        assert!(close(layers[1].3, 2.5) && close(layers[1].4, 20.0), "{:?}", layers[1]);
    }

    #[test]
    fn empty_first_layer_print_time_adds_one_layer_of_overhead() {
        let time = |on: bool| {
            let (mut job, prepared) = empty_first_layer_job(4, 4, "");
            if !on {
                job.metadata_json = job
                    .metadata_json
                    .replace(r#""emptyFirstLayer": true"#, r#""emptyFirstLayer": false"#);
            }
            let bytes = build_ctb_container_bytes(&job, &prepared).unwrap();
            f64::from(u32::from_le_bytes(bytes[0x4c..0x50].try_into().unwrap()))
        };
        // 0.01 s of exposure, 0.1 mm up at 60 mm/min and down at 90 mm/min, plus the
        // fixed per-layer overhead.
        let expected = 0.01 + 0.1 / 60.0 * 60.0 + 0.1 / 90.0 * 60.0
            + f64::from(super::ctb_types::CTB_PER_LAYER_OVERHEAD_SEC);
        assert!((time(true) - time(false) - expected).abs() <= 1.0, "{} vs {}", time(true), time(false));
    }

    #[test]
    fn empty_first_layer_is_written_in_encrypted_v5() {
        let (mut job, prepared) = empty_first_layer_job(5, 3, "");
        job.format_version = Some("v5enc".to_string());
        let bytes = build_ctb_container_bytes(&job, &prepared).expect("v5enc should build");
        // Encrypted layer definitions start with their size (88) then the Z position;
        // the exposure follows, so the blank layer is the one at 0.01 s.
        let mut found = Vec::new();
        for o in 0..bytes.len().saturating_sub(48) {
            let data_len = u32::from_le_bytes(bytes[o + 24..o + 28].try_into().unwrap());
            // 3 bytes for a test layer's RLE, 5 for the blank layer's.
            if u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()) == 88
                && (data_len == 3 || data_len == 5)
            {
                found.push((
                    f32::from_le_bytes(bytes[o + 4..o + 8].try_into().unwrap()),
                    f32::from_le_bytes(bytes[o + 8..o + 12].try_into().unwrap()),
                    f32::from_le_bytes(bytes[o + 40..o + 44].try_into().unwrap()),
                ));
            }
        }
        assert_eq!(found.len(), 4, "{found:?}");
        // Blank layer first: same Z as the real first layer, 0.01 s, 0.1 mm hop.
        assert!(close(found[0].0, 0.05) && close(found[0].1, 0.01) && close(found[0].2, 0.1), "{found:?}");
        assert!(close(found[1].0, 0.05) && close(found[1].1, 30.0) && close(found[1].2, 6.0), "{found:?}");
    }
}
