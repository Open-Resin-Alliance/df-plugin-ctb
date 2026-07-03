import type {
    ComplexPluginDefinition,
    PluginLocalMaterialSettingsAdapterContract,
    MaterialSettingsSource,
} from '@/features/plugins/complexPluginContracts';
import { resolveDifferentialMaterialSettings } from '@/features/plugins/resolveDifferentialSettings';
import { CTB_PLUGIN_MANIFEST } from './pluginManifest';
import { CTB_FORMAT_DEFINITION } from './slicing/ctbFormatDefinition';
import ctbSimpleMaterialSettings from './materialSettings/settings_simple.json';
import ctbTwostageMaterialSettings from './materialSettings/settings_twostage.json';
import ctbBetaSimpleMaterialSettings from './materialSettings/settings_betaonestep.json';
import ctbTwostageDiffMaterialSettings from './materialSettings/settings_twostage.diff.json';
import ctbBetaSimpleDiffMaterialSettings from './materialSettings/settings_betaonestep.diff.json';
import ctbTiltingDiffMaterialSettings from './materialSettings/settings_tilting.diff.json';


function createCtbModeSettingsAdapter(
    modeName: string,
    allModeSources: Record<string, MaterialSettingsSource>,
): PluginLocalMaterialSettingsAdapterContract {
    const source = allModeSources[modeName];
    if (!source) {
        throw new Error(`[CTB] Settings mode "${modeName}" not found in mode sources`);
    }
    const resolved = resolveDifferentialMaterialSettings(source, allModeSources);
    return {
        outputFormat: CTB_FORMAT_DEFINITION.outputFormat,
        ...resolved,
    };
}

const CTB_MODE_SOURCES: Record<string, MaterialSettingsSource> = {
    simple: ctbSimpleMaterialSettings as MaterialSettingsSource,
    twostage: ctbTwostageDiffMaterialSettings as MaterialSettingsSource,
    betaonestep: ctbBetaSimpleDiffMaterialSettings as MaterialSettingsSource,
    tilting: ctbTiltingDiffMaterialSettings as MaterialSettingsSource,
};

const CTB_LOCAL_MATERIAL_SETTINGS_SIMPLE_ADAPTER = createCtbModeSettingsAdapter('simple', CTB_MODE_SOURCES);
const CTB_LOCAL_MATERIAL_SETTINGS_TWOSTAGE_ADAPTER = createCtbModeSettingsAdapter('twostage', CTB_MODE_SOURCES);
const CTB_LOCAL_MATERIAL_SETTINGS_BETA_SIMPLE_ADAPTER = createCtbModeSettingsAdapter('betaonestep', CTB_MODE_SOURCES);
const CTB_LOCAL_MATERIAL_SETTINGS_TILTING_ADAPTER = createCtbModeSettingsAdapter('tilting', CTB_MODE_SOURCES);

export const CTB_COMPLEX_PLUGIN_DEFINITION: ComplexPluginDefinition = {
    id: 'ctb',
    manifest: CTB_PLUGIN_MANIFEST,
    capabilities: {
        networkOperations: false,
        uploadWithProgress: false,
        slicerEncoder: true,
        tauriRuntimePlugin: false,
    },
    slicingFormatsByOutput: {
        [CTB_FORMAT_DEFINITION.outputFormat]: CTB_FORMAT_DEFINITION,
    },
    localMaterialSettingsByOutput: {
        [CTB_FORMAT_DEFINITION.outputFormat]: CTB_LOCAL_MATERIAL_SETTINGS_SIMPLE_ADAPTER,
    },
    localMaterialSettingsByOutputAndMode: {
        [CTB_FORMAT_DEFINITION.outputFormat]: {
            simple: CTB_LOCAL_MATERIAL_SETTINGS_SIMPLE_ADAPTER,
            twostage: CTB_LOCAL_MATERIAL_SETTINGS_TWOSTAGE_ADAPTER,
            betaonestep: CTB_LOCAL_MATERIAL_SETTINGS_BETA_SIMPLE_ADAPTER,
            tilting: CTB_LOCAL_MATERIAL_SETTINGS_TILTING_ADAPTER,
        },
    },
};

export default CTB_COMPLEX_PLUGIN_DEFINITION;
