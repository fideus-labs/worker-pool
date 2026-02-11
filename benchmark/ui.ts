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
  const variantLabel = p.variant === 'vanilla' ? 'Vanilla'
    : p.variant === 'worker-sab' ? 'Worker+SAB'
    : p.variant === 'worker-cache' ? 'Worker+Cache'
    : 'Worker'
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
  const hasCache = result.results.some((r) => r.variant === 'worker-cache')

  // Build dynamic table header
  const thead = $('results-head')
  let headerCols = '<th>Operation</th><th>Vanilla (ms)</th><th>Worker (ms)</th>'
  if (hasSab) headerCols += '<th>Worker+SAB (ms)</th>'
  if (hasCache) headerCols += '<th>Worker+Cache (ms)</th>'
  headerCols += '<th>Speedup (W)</th>'
  if (hasSab) headerCols += '<th>Speedup (SAB)</th>'
  if (hasCache) headerCols += '<th>Speedup (Cache)</th>'
  thead.innerHTML = `<tr>${headerCols}</tr>`

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
    const cached = result.results.find(
      (r) => r.operation === op && r.variant === 'worker-cache',
    )

    if (!vanilla || !worker) continue

    const speedup = vanilla.stats.median / worker.stats.median
    const speedupClass = fmtSpeedupClass(speedup)

    const fmtStat = (r: OperationResult) =>
      `${fmtMs(r.stats.median)} <span style="color:var(--text-dim)">&plusmn;${fmtMs(r.stats.stddev)}</span>`

    let cells = `
      <td>${op === 'read' ? 'Read (get)' : 'Write (set)'}</td>
      <td>${fmtStat(vanilla)}</td>
      <td>${fmtStat(worker)}</td>
    `
    if (hasSab) cells += `<td>${sab ? fmtStat(sab) : '—'}</td>`
    if (hasCache) cells += `<td>${cached ? fmtStat(cached) : '—'}</td>`

    cells += `<td><span class="${speedupClass}">${speedup.toFixed(2)}x</span></td>`

    if (hasSab) {
      if (sab) {
        const sabSpeedup = vanilla.stats.median / sab.stats.median
        cells += `<td><span class="${fmtSpeedupClass(sabSpeedup)}">${sabSpeedup.toFixed(2)}x</span></td>`
      } else {
        cells += '<td>—</td>'
      }
    }
    if (hasCache) {
      if (cached) {
        const cacheSpeedup = vanilla.stats.median / cached.stats.median
        cells += `<td><span class="${fmtSpeedupClass(cacheSpeedup)}">${cacheSpeedup.toFixed(2)}x</span></td>`
      } else {
        cells += '<td>—</td>'
      }
    }

    const row = document.createElement('tr')
    row.innerHTML = cells
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

  const hasCache = result.results.some((r) => r.variant === 'worker-cache')
  if (hasCache) {
    const cacheData = ops.map((op) => {
      const r = result.results.find(
        (r) => r.operation === op && r.variant === 'worker-cache',
      )
      return r?.stats.median ?? 0
    })
    datasets.push({
      label: 'Worker + Cache',
      data: cacheData,
      backgroundColor: 'rgba(134, 239, 172, 0.7)',
      borderColor: 'rgba(134, 239, 172, 1)',
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
    const cached = result.results.find(
      (r) => r.operation === op && r.variant === 'worker-cache',
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
    statsGrid.innerHTML = buildStatsGrid(vanilla, worker, sab, cached)
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

    if (cached) {
      datasets.push({
        label: 'Worker + Cache',
        data: cached.stats.raw,
        backgroundColor: 'rgba(134, 239, 172, 0.5)',
        borderColor: 'rgba(134, 239, 172, 1)',
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
  cached?: OperationResult,
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
        ${cached ? `<div class="stat-value" style="color:#86efac">${fmt(cached.stats)}</div>` : ''}
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
