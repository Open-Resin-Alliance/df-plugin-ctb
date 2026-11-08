import type { SlicingFormatDefinition } from '@/features/slicing/formats/types';

export const CTB_FORMAT_DEFINITION: SlicingFormatDefinition = {
  id: 'ctb.ctb.v1',
  outputFormat: '.ctb',
  displayName: 'CTB',
  ownership: 'plugin',
  layerDataKind: 'raw-mask',
  pluginId: 'ctb',
  layerSettingsFields: [
    'exposureSec', 'lightOffDelaySec', 'waitTimeBeforeCureSec',
    'waitTimeAfterCureSec', 'waitTimeAfterLiftSec', 'liftHeightMm',
    'liftHeight2Mm', 'liftSpeedMmMin', 'liftSpeed2MmMin',
    'retractHeight2Mm', 'retractSpeedMmMin', 'retractSpeed2MmMin', 'lightPwm',
  ],
  formatVersions: [
    { value: 'v2', label: 'V2', layerSettingsFields: ['exposureSec', 'lightOffDelaySec'] },
    { value: 'v3', label: 'V3' },
    { value: 'v4', label: 'V4' },
    { value: 'v5', label: 'V5', isDefault: true },
    { value: 'v3enc', label: 'V3 ENC' },
    { value: 'v4enc', label: 'V4 ENC' },
    { value: 'v5enc', label: 'V5 ENC' },
  ],
  settingsModes: [
    { value: 'simple', label: 'Simple', isDefault: true },
    { value: 'twostage', label: 'Two Stage' },
    { value: 'tilting', label: 'Tilting Vat' },
    { value: 'allfields', label: 'All Fields Single Step Motion'}
  ],
  rustModulePath: 'formats::ctb',
  wasmExportName: 'encode_ctb_container',
  notes: 'CTB binary encoder using raw raster mask layers in dragonfruit-slicer-v3.',
};
