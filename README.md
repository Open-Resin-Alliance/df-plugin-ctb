# df-plugin-ctb

DragonFruit Plugin for the CTB Encoder

## Current status

- Plugin role: **encoder-only** (no network/runtime protocol surface).
- Output target: `.ctb`
- Encoder path: raw raster mask layers from `dragonfruit-slicer-v3` (PNG path disabled).
- Implementation stage: concrete CTB binary serialization enabled (real CTB magic/header, print/slicer tables, layer definitions, CTB RLE packets).

## Projector PWM

*Normal Projector PWM* and *Bottom Projector PWM* are both available in every settings
mode. The bottom value is written per layer and into the file header; when it is not set
it defaults to 100%.

## Wait Times

All wait values live in one **Wait Times** card, in every settings mode:

- **Bottom Wait Layers**: how many layers from the bottom use the bottom values (raised to the bottom layer count if set lower).
- **Bottom / Normal Wait Before Cure**, **After Cure** and **After Lift**.
- **Layer 1 / Layer 2 and Transition Schedule** (optional switch, wait before cure only): own values for the first two layers (0 = same as the bottom wait), the bottom wait held up to **Hold Bottom Wait Up To (mm)**, then a step down to the normal wait either by a fixed **Decrease Per Layer (s)** or linearly **Over a distance (mm)**.

With the schedule off, wait before cure is the plain bottom/normal split, like the other two waits.

Wait values are written into each layer's extended definition and per-layer settings are switched on automatically whenever a bottom wait or the schedule is used (CTB v3+; v2 has no per-layer data). The header's global wait before cure stays at the normal value, and the print-time estimate includes all per-layer waits and lift/retract travel. Logic: `slicing/rust/ctb_wait_profile.rs`.

## Empty First Layer

For printers that ignore the first layer's wait times (and other per-layer settings).
**Empty First Layer** (Wait Times card) prepends one throwaway layer, the way UVTools
does, so the real first layer is no longer the printer's first layer:

- it sits at the same Z as the real first layer, so nothing gets taller or thicker,
- it exposes for 0.01 s at PWM 1 with a single lit pixel in the middle of the screen,
  so nothing cures but the layer is not an empty image firmware might skip,
- it has no light-off delay and no waits, and hops 0.1 mm at the speeds the real first
  layer uses,
- the header's layer count and bottom layer count each go up by one, while the total
  height stays as it was.

Because every real layer moves one position down the file, and CTB v5 obfuscates layer
data with a key derived from the layer's position, the real layers' RLE is re-keyed to
their new positions. Works with the wait schedule and the first-layer movement modes.
Per-layer settings are switched on automatically. Logic: `slicing/rust/ctb_empty_layer.rs`.

## First Layer Movement

**First Layer Movement** (Bottom Layers section) has the same three modes as the Whale3 GCode Generator's CWS output:

- **Default**: unchanged; layer 1 uses the bottom lift and cures one layer height above home.
- **Special: layer 1 one layer below home**: lift 2.5 mm, retract 2.5 mm plus one layer height.
- **Return to home: layer 1 at home**: lift 2.5 mm, retract 2.5 mm.

Both Whale3 modes move at 20 mm/min. The 2.5 mm move is written into layer 1's own lift values, and every layer's Z is moved down with layer 1 (one layer height for home, two for below home; below home gives layer 1 a Z of minus one layer height), so layer thicknesses stay the same. Per-layer settings are switched on automatically. Logic: `slicing/rust/ctb_first_layer.rs`.

## Legal notice (interoperability)

This plugin includes format-compatibility work for CTB-family resin files to enable interoperability between software ecosystems.

The project is developed in good faith for compatibility use cases, with attention to applicable legal frameworks such as:

- EU Directive 2009/24/EC (interoperability-related reverse engineering allowances)
- DMCA Section 1201(f) (United States interoperability exemption)
- Fair Use / Fair Dealing doctrines where applicable

The implementation follows clean-room style engineering practices for independent behavior verification and format compatibility.

Users are responsible for ensuring their use complies with applicable law in their jurisdiction.

**Disclaimer:** This section is general information only and does not constitute legal advice. For jurisdiction-specific guidance, consult qualified legal counsel.
