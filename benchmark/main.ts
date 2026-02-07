/**
 * Benchmark app bootstrap — wires UI events to the benchmark runner.
 */

import { REMOTE_DATASETS, formatRemoteInfo } from './remote.js'
import { formatSyntheticInfo } from './synthetic.js'
import { runBenchmark, cancelBenchmark } from './bench-runner.js'
import {
  showProgress,
  hideProgress,
  updateProgress,
  displayResults,
  clearResults,
  setRunning,
} from './ui.js'
import type {
  BenchmarkConfig,
  BenchmarkOperation,
  SyntheticConfig,
  SupportedDtype,
  DatasetConfig,
} from './types.js'

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

const $ = <T extends HTMLElement>(id: string) =>
  document.getElementById(id) as T

// ---------------------------------------------------------------------------
// Dataset source toggle
// ---------------------------------------------------------------------------

function initDatasetToggle(): void {
  const radios = document.querySelectorAll<HTMLInputElement>(
    'input[name="dataset-source"]',
  )
  const synOptions = $('synthetic-options')
  const remoteOptions = $('remote-options')
  const opWrite = $<HTMLInputElement>('op-write')

  for (const radio of radios) {
    radio.addEventListener('change', () => {
      if (radio.value === 'synthetic') {
        synOptions.classList.remove('hidden')
        remoteOptions.classList.add('hidden')
        opWrite.disabled = false
      } else {
        synOptions.classList.add('hidden')
        remoteOptions.classList.remove('hidden')
        // Write not supported for remote datasets
        opWrite.checked = false
        opWrite.disabled = true
      }
    })
  }
}

// ---------------------------------------------------------------------------
// Remote dataset info
// ---------------------------------------------------------------------------

function initRemoteInfo(): void {
  const select = $<HTMLSelectElement>('remote-dataset')
  const info = $('remote-info')

  function update(): void {
    const preset = REMOTE_DATASETS[select.value]
    if (preset) {
      info.textContent = formatRemoteInfo(preset)
    }
  }

  select.addEventListener('change', update)
  update()
}

// ---------------------------------------------------------------------------
// Read config from UI
// ---------------------------------------------------------------------------

function readConfig(): BenchmarkConfig {
  const source = document.querySelector<HTMLInputElement>(
    'input[name="dataset-source"]:checked',
  )!.value as 'synthetic' | 'remote'

  let dataset: DatasetConfig

  if (source === 'synthetic') {
    const shapeStr = $<HTMLInputElement>('syn-shape').value
    const chunksStr = $<HTMLInputElement>('syn-chunks').value
    const dtype = $<HTMLSelectElement>('syn-dtype').value as SupportedDtype

    const shape = shapeStr.split(',').map((s) => parseInt(s.trim(), 10))
    const chunkShape = chunksStr.split(',').map((s) => parseInt(s.trim(), 10))

    dataset = {
      source: 'synthetic',
      label: `Synthetic ${dtype} [${shape.join('x')}]`,
      shape,
      chunkShape,
      dtype,
    } satisfies SyntheticConfig
  } else {
    const presetKey = $<HTMLSelectElement>('remote-dataset').value
    dataset = REMOTE_DATASETS[presetKey]
  }

  const poolSize = parseInt($<HTMLInputElement>('pool-size').value, 10)
  const iterations = parseInt($<HTMLInputElement>('iterations').value, 10)
  const warmupRuns = parseInt($<HTMLInputElement>('warmup').value, 10)

  const operations: BenchmarkOperation[] = []
  if ($<HTMLInputElement>('op-read').checked) operations.push('read')
  if ($<HTMLInputElement>('op-write').checked) operations.push('write')

  return {
    dataset,
    poolSize,
    iterations,
    warmupRuns,
    operations,
  }
}

// ---------------------------------------------------------------------------
// Run / Cancel
// ---------------------------------------------------------------------------

async function handleRun(): Promise<void> {
  const config = readConfig()

  if (config.operations.length === 0) {
    alert('Select at least one operation (Read or Write).')
    return
  }

  clearResults()
  setRunning(true)
  showProgress()

  try {
    const result = await runBenchmark(config, updateProgress)
    displayResults(result)
  } catch (err) {
    if (err instanceof DOMException && err.name === 'AbortError') {
      console.log('Benchmark cancelled.')
    } else {
      console.error('Benchmark failed:', err)
      alert(`Benchmark failed: ${err instanceof Error ? err.message : String(err)}`)
    }
  } finally {
    setRunning(false)
    hideProgress()
  }
}

function handleCancel(): void {
  cancelBenchmark()
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

function init(): void {
  initDatasetToggle()
  initRemoteInfo()

  // Default pool size to the number of logical cores (capped at 128)
  const poolSizeInput = $<HTMLInputElement>('pool-size')
  poolSizeInput.value = String(Math.min(navigator?.hardwareConcurrency || 4, 128))

  $('btn-run').addEventListener('click', handleRun)
  $('btn-cancel').addEventListener('click', handleCancel)
}

// Start when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init)
} else {
  init()
}
