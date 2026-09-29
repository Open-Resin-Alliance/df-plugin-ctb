mod ctb_crypto;
mod ctb_layout;
mod ctb_metadata;
mod ctb_preview;
mod ctb_types;
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
use ctb_metadata::{
    parse_ctb_build_model_from_job, parse_ctb_format_version_hint_from_job,
    parse_threshold_from_metadata,
};
use std::io::{Read, Seek, Write};
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
/// streams the already-encoded payload straight to a scratch file as each
/// layer arrives.
///
/// Only a lightweight `CtbLayerRecord` (index + encoded length) is kept in
/// memory per layer for the whole job — never the payload bytes. That's what
/// stops 3DAA laminations at 16k+ resolution from accumulating gigabytes of
/// encoded-but-unwritten layer data in RAM over the course of a job (the
/// previous `prepared: Vec<CtbPreparedLayer>` design held every layer's bytes
/// until the very end).
///
/// Layers can arrive out of order — 3DAA's post pipeline dispatches each
/// layer's encoding as an independent parallel task — but the scratch file
/// needs them written in strict index order, since each layer's on-disk
/// offset is the running sum of every *prior* layer's length. `pending`
/// stages out-of-order arrivals; in practice it never grows beyond the
/// pipeline's own small reordering window.
struct CtbRleStreamingEncoder {
    job: SliceJobV3,
    layer_xor_key: u32,
    is_anti_aliased: bool,
    threshold: u8,
    total_pixels: usize,
    total_layers: usize,
    payload_file: std::fs::File,
    next_index: usize,
    pending: std::collections::BTreeMap<usize, (ctb_types::CtbLayerRecord, Vec<u8>)>,
    records: Vec<ctb_types::CtbLayerRecord>,
    io_error: Option<SlicerV3Error>,
}

impl CtbRleStreamingEncoder {
    fn new(
        job: &SliceJobV3,
        layer_xor_key: u32,
        is_anti_aliased: bool,
        threshold: u8,
        total_pixels: usize,
    ) -> Result<Self, SlicerV3Error> {
        Ok(Self {
            job: job.clone(),
            layer_xor_key,
            is_anti_aliased,
            threshold,
            total_pixels,
            total_layers: job.total_layers as usize,
            payload_file: tempfile::tempfile()?,
            next_index: 0,
            pending: std::collections::BTreeMap::new(),
            records: Vec::with_capacity(job.total_layers as usize),
            io_error: None,
        })
    }

    fn on_layer_encoded(&mut self, layer_index: usize, encoded: Vec<u8>) {
        if self.io_error.is_some() {
            return;
        }
        let record = ctb_types::CtbLayerRecord {
            index: layer_index,
            source_len: self.total_pixels,
            encoded_len: encoded.len() as u32,
        };
        self.pending.insert(layer_index, (record, encoded));

        while let Some((&idx, _)) = self.pending.iter().next() {
            if idx != self.next_index {
                break;
            }
            let (record, bytes) = self.pending.remove(&idx).expect("checked above");
            if let Err(err) = self.payload_file.write_all(&bytes) {
                self.io_error = Some(SlicerV3Error::from(err));
                return;
            }
            self.records.push(record);
            self.next_index += 1;
        }
    }

    /// Verifies every layer 0..total_layers was written in order, with
    /// nothing left staged out-of-order, and surfaces any deferred I/O error
    /// from `store_encoded_layer` (which can't propagate errors directly —
    /// see its `RleStreamEncoder` signature).
    fn finish(&mut self) -> Result<(), SlicerV3Error> {
        if let Some(err) = self.io_error.take() {
            return Err(err);
        }
        if self.next_index != self.total_layers || !self.pending.is_empty() {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(format!(
                "CTB streaming encoder finished with {} of {} layers written ({} layer(s) still buffered out of order)",
                self.next_index,
                self.total_layers,
                self.pending.len()
            )));
        }
        Ok(())
    }

    /// Reads the scratch file back into memory and reconstructs the
    /// `CtbPreparedLayer` list the existing (non-streaming) container
    /// builders expect. Used by `finalize_to_bytes` and by the encrypted
    /// `finalize_to_path` fallback, neither of which is the hot path for
    /// large desktop exports.
    fn read_back_prepared(&mut self) -> Result<Vec<ctb_types::CtbPreparedLayer>, SlicerV3Error> {
        self.payload_file.seek(std::io::SeekFrom::Start(0))?;
        let mut payload_bytes = Vec::new();
        self.payload_file.read_to_end(&mut payload_bytes)?;

        let mut prepared = Vec::with_capacity(self.records.len());
        let mut cursor = 0usize;
        for record in &self.records {
            let end = cursor + record.encoded_len as usize;
            prepared.push(ctb_types::CtbPreparedLayer {
                index: record.index,
                source_len: record.source_len,
                encoded: payload_bytes[cursor..end].to_vec(),
            });
            cursor = end;
        }
        Ok(prepared)
    }
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
        self.on_layer_encoded(layer_index as usize, encoded);
        if let Some(err) = self.io_error.take() {
            return Err(err);
        }
        Ok(())
    }

    fn finalize_to_bytes(mut self: Box<Self>) -> Result<Vec<u8>, SlicerV3Error> {
        self.finish()?;
        if self.records.is_empty() {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "no rendered layers were provided for CTB RLE encoding".to_string(),
            ));
        }
        let prepared = self.read_back_prepared()?;
        build_ctb_container_bytes(&self.job, &prepared)
    }

    /// Streams the container straight to `output_path` without ever holding
    /// the full encoded file in RAM — this is the path the desktop app uses
    /// for real exports (`slice_with_progress_v3_to_path`).
    ///
    /// Only the non-encrypted CTB v5 layout is streamed this way: its layer
    /// offsets are purely forward-computable, so the header, the layer
    /// descriptor table, and the (potentially huge) payload region can all be
    /// written in one forward pass. The encrypted (v5enc) layout patches a
    /// layer-pointer table and settings block after the fact, which needs
    /// random access — that variant falls back to the same in-memory
    /// assembly as `finalize_to_bytes`, just written to `output_path`
    /// afterwards. Either way, the sustained per-layer growth during
    /// lamination is gone; only encrypted jobs still pay a one-time,
    /// end-of-job buffering cost for the final container.
    fn finalize_to_path(mut self: Box<Self>, output_path: &Path) -> Result<(), SlicerV3Error> {
        self.finish()?;
        if self.records.is_empty() {
            return Err(SlicerV3Error::MissingRenderedLayerPayload(
                "no rendered layers were provided for CTB RLE encoding".to_string(),
            ));
        }

        let force_encrypted = parse_ctb_format_version_hint_from_job(&self.job)
            .map(|(_, is_encrypted)| is_encrypted)
            .unwrap_or(false);

        if force_encrypted {
            let prepared = self.read_back_prepared()?;
            let bytes = build_ctb_container_bytes(&self.job, &prepared)?;
            std::fs::write(output_path, bytes)?;
            return Ok(());
        }

        self.payload_file.seek(std::io::SeekFrom::Start(0))?;
        let reader = std::io::BufReader::new(&mut self.payload_file);
        let mut writer = std::io::BufWriter::new(std::fs::File::create(output_path)?);
        ctb_encoder::write_ctb_v5_container(&self.job, &self.records, reader, &mut writer, None)?;
        writer.flush()?;
        Ok(())
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
        self.on_layer_encoded(layer_index as usize, bytes);
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
        Ok(Some(Box::new(CtbRleStreamingEncoder::new(
            job,
            build.layer_xor_key,
            is_anti_aliased,
            threshold,
            total_pixels,
        )?)))
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
        CtbRleStreamingEncoder, CTB_DISCLAIMER_SIZE, CTB_HEADER_SIZE,
    };
    use crate::encoders::RleStreamEncoder;
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

    #[test]
    fn streaming_encoder_finalize_to_path_matches_finalize_to_bytes() {
        let mut job = make_test_job();
        job.total_layers = 3;
        let total_pixels =
            (job.source_width_px as usize).saturating_mul(job.source_height_px as usize);
        let layer_bytes = |n: u8| vec![0x80u8, 0x03, n];

        let mut path_encoder =
            CtbRleStreamingEncoder::new(&job, 0x1234_5678, true, 127, total_pixels)
                .expect("encoder should construct");
        for i in 0..3u32 {
            path_encoder.store_encoded_layer(i, layer_bytes(i as u8 + 1));
        }
        let tmp = tempfile::NamedTempFile::new().expect("temp output path");
        Box::new(path_encoder)
            .finalize_to_path(tmp.path())
            .expect("finalize_to_path should succeed");
        let path_bytes = std::fs::read(tmp.path()).expect("read streamed output");

        let mut bytes_encoder =
            CtbRleStreamingEncoder::new(&job, 0x1234_5678, true, 127, total_pixels)
                .expect("encoder should construct");
        for i in 0..3u32 {
            bytes_encoder.store_encoded_layer(i, layer_bytes(i as u8 + 1));
        }
        let mem_bytes = Box::new(bytes_encoder)
            .finalize_to_bytes()
            .expect("finalize_to_bytes should succeed");

        assert_eq!(
            path_bytes, mem_bytes,
            "streaming finalize_to_path and in-memory finalize_to_bytes must produce byte-identical containers"
        );
    }

    #[test]
    fn streaming_encoder_handles_out_of_order_store_encoded_layer() {
        let mut job = make_test_job();
        job.total_layers = 5;
        let total_pixels =
            (job.source_width_px as usize).saturating_mul(job.source_height_px as usize);
        let layer_bytes = |n: u8| vec![0x80u8, 0x03, n];

        let mut ordered = CtbRleStreamingEncoder::new(&job, 0xABCD, true, 127, total_pixels)
            .expect("encoder should construct");
        for i in 0..5u32 {
            ordered.store_encoded_layer(i, layer_bytes(i as u8 + 1));
        }
        let ordered_bytes = Box::new(ordered)
            .finalize_to_bytes()
            .expect("ordered finalize should succeed");

        let mut scrambled = CtbRleStreamingEncoder::new(&job, 0xABCD, true, 127, total_pixels)
            .expect("encoder should construct");
        for &i in &[2u32, 0, 1, 4, 3] {
            scrambled.store_encoded_layer(i, layer_bytes(i as u8 + 1));
        }
        let scrambled_bytes = Box::new(scrambled)
            .finalize_to_bytes()
            .expect("scrambled finalize should succeed");

        assert_eq!(
            ordered_bytes, scrambled_bytes,
            "out-of-order store_encoded_layer arrivals must still produce an index-ordered container"
        );
    }

    #[test]
    fn streaming_encoder_rejects_missing_layer_at_finalize() {
        let mut job = make_test_job();
        job.total_layers = 3;
        let total_pixels =
            (job.source_width_px as usize).saturating_mul(job.source_height_px as usize);

        let mut encoder = CtbRleStreamingEncoder::new(&job, 0xABCD, true, 127, total_pixels)
            .expect("encoder should construct");
        // Only 2 of the 3 expected layers arrive (index 1 is missing).
        encoder.store_encoded_layer(0, vec![0x80, 0x03, 1]);
        encoder.store_encoded_layer(2, vec![0x80, 0x03, 3]);

        let result = Box::new(encoder).finalize_to_bytes();
        assert!(
            result.is_err(),
            "finalize should fail when a layer is missing rather than silently emitting a truncated container"
        );
    }
}
