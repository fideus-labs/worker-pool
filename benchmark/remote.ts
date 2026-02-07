/**
 * Remote OME-Zarr dataset presets from AWS S3.
 *
 * All datasets are from the InsightSoftwareConsortium OME-Zarr Scientific
 * Visualization collection. They use zarr v3 with bytes + zstd codecs.
 *
 * @see https://github.com/InsightSoftwareConsortium/OMEZarrOpenSciVisDatasets
 */

import type { RemoteConfig } from './types.js'

const BASE_URL =
  'https://ome-zarr-scivis.s3.us-east-1.amazonaws.com/v0.5/96x2'

export const REMOTE_DATASETS: Record<string, RemoteConfig> = {
  fuel: {
    source: 'remote',
    label: 'fuel',
    rootUrl: `${BASE_URL}/fuel.ome.zarr`,
    arrayPath: 'scale0/fuel',
    shape: [64, 64, 64],
    chunkShape: [32, 64, 64],
    dtype: 'uint8',
    numChunks: 2,
  },
  engine: {
    source: 'remote',
    label: 'engine',
    rootUrl: `${BASE_URL}/engine.ome.zarr`,
    arrayPath: 'scale0/engine',
    shape: [128, 256, 256],
    chunkShape: [32, 128, 128],
    dtype: 'uint8',
    numChunks: 16,
  },
  aneurism: {
    source: 'remote',
    label: 'aneurism',
    rootUrl: `${BASE_URL}/aneurism.ome.zarr`,
    arrayPath: 'scale0/aneurism',
    shape: [256, 256, 256],
    chunkShape: [64, 64, 128],
    dtype: 'uint8',
    numChunks: 32,
  },
  bonsai: {
    source: 'remote',
    label: 'bonsai',
    rootUrl: `${BASE_URL}/bonsai.ome.zarr`,
    arrayPath: 'scale0/bonsai',
    shape: [256, 256, 256],
    chunkShape: [64, 64, 128],
    dtype: 'uint8',
    numChunks: 32,
  },
  backpack: {
    source: 'remote',
    label: 'backpack',
    rootUrl: `${BASE_URL}/backpack.ome.zarr`,
    arrayPath: 'scale0/backpack',
    shape: [512, 512, 373],
    chunkShape: [64, 128, 128],
    dtype: 'uint16',
    numChunks: 96,
  },
  bunny: {
    source: 'remote',
    label: 'bunny',
    rootUrl: `${BASE_URL}/bunny.ome.zarr`,
    arrayPath: 'scale0/bunny',
    shape: [512, 512, 361],
    chunkShape: [64, 128, 128],
    dtype: 'uint16',
    numChunks: 96,
  },
}

/**
 * Format a remote dataset config as a human-readable info string.
 */
export function formatRemoteInfo(config: RemoteConfig): string {
  const totalBytes = config.shape.reduce((a, b) => a * b, 1) *
    (config.dtype.includes('16') ? 2 : config.dtype.includes('32') ? 4 : config.dtype.includes('64') ? 8 : 1)
  const mb = (totalBytes / (1024 * 1024)).toFixed(1)
  return [
    `Shape: [${config.shape.join(', ')}]`,
    `Chunks: [${config.chunkShape.join(', ')}]`,
    `Dtype: ${config.dtype}`,
    `Chunks: ~${config.numChunks}`,
    `Size: ~${mb} MB (uncompressed)`,
    `Codecs: bytes + zstd`,
  ].join('\n')
}
