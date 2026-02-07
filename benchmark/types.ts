/**
 * Types for the benchmark app.
 */

// ---------------------------------------------------------------------------
// Dataset configuration
// ---------------------------------------------------------------------------

export type DatasetSource = 'synthetic' | 'remote'

export interface SyntheticConfig {
  source: 'synthetic'
  label: string
  shape: number[]
  chunkShape: number[]
  dtype: SupportedDtype
}

export interface RemoteConfig {
  source: 'remote'
  label: string
  /** Full URL to the zarr root (group level, e.g. ".../fuel.ome.zarr") */
  rootUrl: string
  /** Path to the array within the zarr group (e.g. "scale0/fuel") */
  arrayPath: string
  /** Expected shape for display */
  shape: number[]
  /** Expected chunk shape for display */
  chunkShape: number[]
  dtype: string
  /** Approximate number of chunks */
  numChunks: number
}

export type DatasetConfig = SyntheticConfig | RemoteConfig

export type SupportedDtype =
  | 'int32'
  | 'uint8'
  | 'uint16'
  | 'float32'
  | 'float64'

// ---------------------------------------------------------------------------
// Benchmark configuration
// ---------------------------------------------------------------------------

export type BenchmarkOperation = 'read' | 'write'

export interface BenchmarkConfig {
  dataset: DatasetConfig
  poolSize: number
  iterations: number
  warmupRuns: number
  operations: BenchmarkOperation[]
}

// ---------------------------------------------------------------------------
// Benchmark results
// ---------------------------------------------------------------------------

export interface TimingStats {
  mean: number
  median: number
  min: number
  max: number
  stddev: number
  raw: number[]
}

export interface OperationResult {
  operation: BenchmarkOperation
  variant: 'vanilla' | 'worker'
  stats: TimingStats
}

export interface BenchmarkResult {
  config: BenchmarkConfig
  results: OperationResult[]
  timestamp: number
}

// ---------------------------------------------------------------------------
// Progress callback
// ---------------------------------------------------------------------------

export interface BenchmarkProgress {
  phase: 'warmup' | 'benchmark'
  operation: BenchmarkOperation
  variant: 'vanilla' | 'worker'
  iteration: number
  totalIterations: number
  /** Overall progress 0..1 */
  progress: number
}

export type ProgressCallback = (progress: BenchmarkProgress) => void
