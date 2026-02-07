/**
 * UI module — DOM manipulation, Chart.js rendering, progress updates.
 */

import {
  Chart,
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  Legend,
  Tooltip,
  Colors,
  type ChartConfiguration,
} from 'chart.js'
import type {
  BenchmarkResult,
  BenchmarkProgress,
  OperationResult,
  TimingStats,
} from './types.js'

// Register Chart.js components (tree-shaken)
Chart.register(
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  Legend,
  Tooltip,
  Colors,
)

// Chart.js dark theme defaults
Chart.defaults.color = '#94a3b8'
Chart.defaults.borderColor = '#2e3346'
Chart.defaults.font.family =
  "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace"

// ---------------------------------------------------------------------------
// DOM references
// ---------------------------------------------------------------------------

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T

let mainChart: Chart | null = null
const detailCharts: Chart[] = []

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

export function showProgress(): void {
  $('progress-container').classList.remove('hidden')
  updateProgress({ phase: 'warmup', operation: 'read', variant: 'vanilla', iteration: 0, totalIterations: 0, progress: 0 })
}

export function hideProgress(): void {
  $('progress-container').classList.add('hidden')
}

export function updateProgress(p: BenchmarkProgress): void {
  const fill = $('progress-fill') as HTMLDivElement
  const label = $('progress-label') as HTMLParagraphElement

  fill.style.width = `${(p.progress * 100).toFixed(1)}%`

  const phaseLabel = p.phase === 'warmup' ? 'Warmup' : 'Benchmark'
  const opLabel = p.operation === 'read' ? 'Read' : 'Write'
  const variantLabel = p.variant === 'vanilla' ? 'Vanilla' : p.variant === 'worker-sab' ? 'Worker+SAB' : 'Worker'
  label.textContent =
    `${phaseLabel} | ${opLabel} (${variantLabel}) | ` +
    `${p.iteration}/${p.totalIterations} | ${(p.progress * 100).toFixed(0)}%`
}

// ---------------------------------------------------------------------------
// Results display
// ---------------------------------------------------------------------------

export function displayResults(result: BenchmarkResult): void {
  $('no-results').classList.add('hidden')

  renderSummaryTable(result)
  renderMainChart(result)
  renderDetailCharts(result)
}

export function clearResults(): void {
  $('chart-container').classList.add('hidden')
  $('table-container').classList.add('hidden')
  $('detail-container').classList.add('hidden')
  $('no-results').classList.remove('hidden')
  $<HTMLTableSectionElement>('results-body').innerHTML = ''

  // Destroy existing charts
  mainChart?.destroy()
  mainChart = null
  for (const c of detailCharts) c.destroy()
  detailCharts.length = 0
  $('detail-charts').innerHTML = ''
}

// ---------------------------------------------------------------------------
// Summary table
// ---------------------------------------------------------------------------

function renderSummaryTable(result: BenchmarkResult): void {
  const tbody = $<HTMLTableSectionElement>('results-body')
  tbody.innerHTML = ''

  const hasSab = result.results.some((r) => r.variant === 'worker-sab')

  // Update table header based on whether SAB column is needed
  const thead = $('results-head')
  if (hasSab) {
    thead.innerHTML = `<tr>
      <th>Operation</th>
      <th>Vanilla (ms)</th>
      <th>Worker (ms)</th>
      <th>Worker+SAB (ms)</th>
      <th>Speedup (W)</th>
      <th>Speedup (SAB)</th>
    </tr>`
  } else {
    thead.innerHTML = `<tr>
      <th>Operation</th>
      <th>Vanilla (ms)</th>
      <th>Worker (ms)</th>
      <th>Speedup</th>
    </tr>`
  }

  // Group results by operation
  const ops = new Set(result.results.map((r) => r.operation))

  for (const op of ops) {
    const vanilla = result.results.find(
      (r) => r.operation === op && r.variant === 'vanilla',
    )
    const worker = result.results.find(
      (r) => r.operation === op && r.variant === 'worker',
    )
    const sab = result.results.find(
      (r) => r.operation === op && r.variant === 'worker-sab',
    )

    if (!vanilla || !worker) continue

    const speedup = vanilla.stats.median / worker.stats.median
    const speedupClass = fmtSpeedupClass(speedup)

    const row = document.createElement('tr')
    if (hasSab && sab) {
      const sabSpeedup = vanilla.stats.median / sab.stats.median
      const sabClass = fmtSpeedupClass(sabSpeedup)
      row.innerHTML = `
        <td>${op === 'read' ? 'Read (get)' : 'Write (set)'}</td>
        <td>${fmtMs(vanilla.stats.median)} <span style="color:var(--text-dim)">&plusmn;${fmtMs(vanilla.stats.stddev)}</span></td>
        <td>${fmtMs(worker.stats.median)} <span style="color:var(--text-dim)">&plusmn;${fmtMs(worker.stats.stddev)}</span></td>
        <td>${fmtMs(sab.stats.median)} <span style="color:var(--text-dim)">&plusmn;${fmtMs(sab.stats.stddev)}</span></td>
        <td><span class="${speedupClass}">${speedup.toFixed(2)}x</span></td>
        <td><span class="${sabClass}">${sabSpeedup.toFixed(2)}x</span></td>
      `
    } else {
      row.innerHTML = `
        <td>${op === 'read' ? 'Read (get)' : 'Write (set)'}</td>
        <td>${fmtMs(vanilla.stats.median)} <span style="color:var(--text-dim)">&plusmn;${fmtMs(vanilla.stats.stddev)}</span></td>
        <td>${fmtMs(worker.stats.median)} <span style="color:var(--text-dim)">&plusmn;${fmtMs(worker.stats.stddev)}</span></td>
        <td><span class="${speedupClass}">${speedup.toFixed(2)}x</span></td>
      `
    }
    tbody.appendChild(row)
  }

  $('table-container').classList.remove('hidden')
}

function fmtSpeedupClass(speedup: number): string {
  return speedup > 1.1
    ? 'speedup-positive'
    : speedup < 0.9
      ? 'speedup-negative'
      : 'speedup-neutral'
}

function fmtMs(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}us`
  if (ms < 1000) return `${ms.toFixed(1)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

// ---------------------------------------------------------------------------
// Main chart — grouped bar chart comparing median times
// ---------------------------------------------------------------------------

function renderMainChart(result: BenchmarkResult): void {
  mainChart?.destroy()

  const container = $('chart-container')
  container.classList.remove('hidden')

  const canvas = $<HTMLCanvasElement>('benchmark-chart')

  const hasSab = result.results.some((r) => r.variant === 'worker-sab')

  // Build labels and data
  const ops = [...new Set(result.results.map((r) => r.operation))]
  const labels = ops.map((op) => (op === 'read' ? 'Read' : 'Write'))

  const vanillaData = ops.map((op) => {
    const r = result.results.find(
      (r) => r.operation === op && r.variant === 'vanilla',
    )
    return r?.stats.median ?? 0
  })

  const workerData = ops.map((op) => {
    const r = result.results.find(
      (r) => r.operation === op && r.variant === 'worker',
    )
    return r?.stats.median ?? 0
  })

  const datasets: ChartConfiguration<'bar'>['data']['datasets'] = [
    {
      label: 'Vanilla zarrita',
      data: vanillaData,
      backgroundColor: 'rgba(128, 128, 128, 0.7)',
      borderColor: 'rgba(128, 128, 128, 1)',
      borderWidth: 1,
      borderRadius: 4,
    },
    {
      label: 'Worker Pool',
      data: workerData,
      backgroundColor: 'rgba(53, 98, 160, 0.7)',
      borderColor: 'rgba(53, 98, 160, 1)',
      borderWidth: 1,
      borderRadius: 4,
    },
  ]

  if (hasSab) {
    const sabData = ops.map((op) => {
      const r = result.results.find(
        (r) => r.operation === op && r.variant === 'worker-sab',
      )
      return r?.stats.median ?? 0
    })
    datasets.push({
      label: 'Worker + SAB',
      data: sabData,
      backgroundColor: 'rgba(249, 232, 162, 0.7)',
      borderColor: 'rgba(249, 232, 162, 1)',
      borderWidth: 1,
      borderRadius: 4,
    })
  }

  const config: ChartConfiguration<'bar'> = {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { position: 'top' },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${fmtMs(ctx.parsed.y)}`,
          },
        },
      },
      scales: {
        x: {
          title: { display: true, text: 'Operation' },
          grid: { display: false },
        },
        y: {
          title: { display: true, text: 'Median Time (ms)' },
          beginAtZero: true,
          grid: { color: 'rgba(46, 51, 70, 0.5)' },
        },
      },
    },
  }

  mainChart = new Chart(canvas, config)
}

// ---------------------------------------------------------------------------
// Detail charts — per-iteration line/bar for each operation
// ---------------------------------------------------------------------------

function renderDetailCharts(result: BenchmarkResult): void {
  for (const c of detailCharts) c.destroy()
  detailCharts.length = 0

  const container = $('detail-charts')
  container.innerHTML = ''

  const ops = [...new Set(result.results.map((r) => r.operation))]

  for (const op of ops) {
    const vanilla = result.results.find(
      (r) => r.operation === op && r.variant === 'vanilla',
    )
    const worker = result.results.find(
      (r) => r.operation === op && r.variant === 'worker',
    )
    const sab = result.results.find(
      (r) => r.operation === op && r.variant === 'worker-sab',
    )
    if (!vanilla || !worker) continue

    // Wrapper
    const wrapper = document.createElement('div')
    wrapper.className = 'detail-chart-wrapper'

    const title = document.createElement('h4')
    title.textContent = `${op === 'read' ? 'Read' : 'Write'} — Per-Iteration Times`
    wrapper.appendChild(title)

    const canvas = document.createElement('canvas')
    wrapper.appendChild(canvas)

    // Stats grid
    const statsGrid = document.createElement('div')
    statsGrid.className = 'stats-grid'
    statsGrid.innerHTML = buildStatsGrid(vanilla, worker, sab)
    wrapper.appendChild(statsGrid)

    container.appendChild(wrapper)

    // Chart
    const labels = vanilla.stats.raw.map((_, i) => `#${i + 1}`)

    const datasets: ChartConfiguration<'bar'>['data']['datasets'] = [
      {
        label: 'Vanilla',
        data: vanilla.stats.raw,
        backgroundColor: 'rgba(128, 128, 128, 0.5)',
        borderColor: 'rgba(128, 128, 128, 1)',
        borderWidth: 1,
        borderRadius: 3,
      },
      {
        label: 'Worker',
        data: worker.stats.raw,
        backgroundColor: 'rgba(53, 98, 160, 0.5)',
        borderColor: 'rgba(53, 98, 160, 1)',
        borderWidth: 1,
        borderRadius: 3,
      },
    ]

    if (sab) {
      datasets.push({
        label: 'Worker + SAB',
        data: sab.stats.raw,
        backgroundColor: 'rgba(249, 232, 162, 0.5)',
        borderColor: 'rgba(249, 232, 162, 1)',
        borderWidth: 1,
        borderRadius: 3,
      })
    }

    const chart = new Chart(canvas, {
      type: 'bar',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { position: 'top' },
          tooltip: {
            callbacks: {
              label: (ctx) => `${ctx.dataset.label}: ${fmtMs(ctx.parsed.y)}`,
            },
          },
        },
        scales: {
          x: {
            title: { display: true, text: 'Iteration' },
            grid: { display: false },
          },
          y: {
            title: { display: true, text: 'Time (ms)' },
            beginAtZero: true,
            grid: { color: 'rgba(46, 51, 70, 0.5)' },
          },
        },
      },
    })

    detailCharts.push(chart)
  }

  $('detail-container').classList.remove('hidden')
}

function buildStatsGrid(
  vanilla: OperationResult,
  worker: OperationResult,
  sab?: OperationResult,
): string {
  const pairs: [string, (s: TimingStats) => string][] = [
    ['Mean', (s) => fmtMs(s.mean)],
    ['Median', (s) => fmtMs(s.median)],
    ['Min', (s) => fmtMs(s.min)],
    ['Max', (s) => fmtMs(s.max)],
    ['Std Dev', (s) => fmtMs(s.stddev)],
  ]

  let html = ''
  for (const [label, fmt] of pairs) {
    html += `
      <div class="stat-card">
        <div class="stat-label">${label}</div>
        <div class="stat-value" style="color:#808080">${fmt(vanilla.stats)}</div>
        <div class="stat-value" style="color:#3562a0">${fmt(worker.stats)}</div>
        ${sab ? `<div class="stat-value" style="color:#f9e8a2">${fmt(sab.stats)}</div>` : ''}
      </div>
    `
  }
  return html
}

// ---------------------------------------------------------------------------
// Button state helpers
// ---------------------------------------------------------------------------

export function setRunning(running: boolean): void {
  const btnRun = $<HTMLButtonElement>('btn-run')
  const btnCancel = $<HTMLButtonElement>('btn-cancel')

  btnRun.disabled = running
  btnRun.textContent = running ? 'Running...' : 'Run Benchmark'
  btnCancel.disabled = !running
}
