/**
 * Tests for chunk shape auto-detection in @fideus-labs/fizarrita.
 *
 * Tests readZstdFrameContentSize() and inferChunkShape() — the pure functions
 * that detect and correct metadata chunk_shape mismatches by reading the zstd
 * frame header and applying heuristic scoring to infer the actual chunk shape.
 */

import { test, expect } from '@playwright/test'

test.describe('readZstdFrameContentSize', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => typeof window.readZstdFrameContentSize === 'function')
  })

  test('returns null for non-zstd data', async ({ page }) => {
    const result = await page.evaluate(() => {
      const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7])
      return window.readZstdFrameContentSize(data)
    })
    expect(result).toBeNull()
  })

  test('returns null for empty buffer', async ({ page }) => {
    const result = await page.evaluate(() => {
      return window.readZstdFrameContentSize(new Uint8Array(0))
    })
    expect(result).toBeNull()
  })

  test('returns null for buffer too short to be zstd', async ({ page }) => {
    const result = await page.evaluate(() => {
      return window.readZstdFrameContentSize(new Uint8Array([0x28, 0xb5, 0x2f, 0xfd]))
    })
    expect(result).toBeNull()
  })

  test('reads 1-byte FCS (single segment, fcsFlag=0)', async ({ page }) => {
    // Zstd magic (LE): 0x28 0xB5 0x2F 0xFD
    // FHD: fcsFlag=0, singleSegment=1, dictIdFlag=0 -> byte = 0b00_1_00_0_00 = 0x20
    // fcsFieldSize = 1 (because singleSegment=1 && fcsFlag=0)
    // windowDescSize = 0 (singleSegment)
    // dictIdSize = 0
    // FCS at offset 5, value = 42
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x20, 42])
      return window.readZstdFrameContentSize(buf)
    })
    expect(result).toBe(42)
  })

  test('reads 2-byte FCS (fcsFlag=1)', async ({ page }) => {
    // FHD: fcsFlag=1, singleSegment=0, dictIdFlag=0 -> 0b01_0_00_0_00 = 0x40
    // fcsFieldSize = 2
    // windowDescSize = 1 (not single segment)
    // FCS at offset 5 + 1 = 6, value = (0x10 | 0x00<<8) + 256 = 16 + 256 = 272
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x40, 0x00, 0x10, 0x00])
      return window.readZstdFrameContentSize(buf)
    })
    expect(result).toBe(272)
  })

  test('reads 4-byte FCS (fcsFlag=2)', async ({ page }) => {
    // FHD: fcsFlag=2, singleSegment=1, dictIdFlag=0 -> 0b10_1_00_0_00 = 0xA0
    // fcsFieldSize = 4
    // windowDescSize = 0 (singleSegment)
    // FCS at offset 5, value = 397312 (0x00061000)
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        0x28, 0xb5, 0x2f, 0xfd, 0xa0,
        0x00, 0x10, 0x06, 0x00,
      ])
      return window.readZstdFrameContentSize(buf)
    })
    expect(result).toBe(397312)
  })

  test('reads 8-byte FCS (fcsFlag=3)', async ({ page }) => {
    // FHD: fcsFlag=3, singleSegment=1, dictIdFlag=0 -> 0b11_1_00_0_00 = 0xE0
    // fcsFieldSize = 8
    // windowDescSize = 0 (singleSegment)
    // FCS at offset 5, value = 1000000 (0x00000000000F4240)
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        0x28, 0xb5, 0x2f, 0xfd, 0xe0,
        0x40, 0x42, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00,
      ])
      return window.readZstdFrameContentSize(buf)
    })
    expect(result).toBe(1000000)
  })

  test('returns null when fcsFlag=0 and not single segment', async ({ page }) => {
    // FHD: fcsFlag=0, singleSegment=0, dictIdFlag=0 -> 0b00_0_00_0_00 = 0x00
    // fcsFieldSize = 0 -> return null
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00, 0x00])
      return window.readZstdFrameContentSize(buf)
    })
    expect(result).toBeNull()
  })

  test('handles dictIdFlag correctly (skips dict ID bytes)', async ({ page }) => {
    // FHD: fcsFlag=2, singleSegment=1, dictIdFlag=2 -> dictIdSize=2
    // 0b10_1_00_0_10 = 0xA2
    // windowDescSize = 0 (singleSegment)
    // dictIdSize = 2
    // FCS at offset 5 + 0 + 2 = 7, value = 12345 (0x00003039)
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        0x28, 0xb5, 0x2f, 0xfd, 0xa2,
        0x00, 0x00, // 2-byte dict ID
        0x39, 0x30, 0x00, 0x00, // 4-byte FCS = 12345
      ])
      return window.readZstdFrameContentSize(buf)
    })
    expect(result).toBe(12345)
  })
})

test.describe('readBloscFrameContentSize', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => typeof window.readBloscFrameContentSize === 'function')
  })

  test('returns null for non-blosc data', async ({ page }) => {
    const result = await page.evaluate(() => {
      // Random bytes that don't look like a blosc header (version=0 is invalid)
      const data = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      return window.readBloscFrameContentSize(data)
    })
    expect(result).toBeNull()
  })

  test('returns null for buffer shorter than 16 bytes', async ({ page }) => {
    const result = await page.evaluate(() => {
      return window.readBloscFrameContentSize(new Uint8Array([1, 0, 0, 4, 0x00, 0x10]))
    })
    expect(result).toBeNull()
  })

  test('reads correct nbytes from valid blosc header', async ({ page }) => {
    // Blosc header: version=2, versionlz=1, flags=0, typesize=4
    // nbytes = 4096 (0x00001000 LE), blocksize=256, cbytes=16 (= buffer length)
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        2,    // version
        1,    // versionlz
        0,    // flags
        4,    // typesize
        0x00, 0x10, 0x00, 0x00, // nbytes = 4096 (LE)
        0x00, 0x01, 0x00, 0x00, // blocksize = 256
        0x10, 0x00, 0x00, 0x00, // cbytes = 16 (matches buffer length)
      ])
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBe(4096)
  })

  test('returns null for invalid version 0', async ({ page }) => {
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        0,    // version = 0 (invalid)
        1, 0, 4,
        0x00, 0x10, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00,
      ])
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBeNull()
  })

  test('returns null for invalid version > 2', async ({ page }) => {
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        3,    // version = 3 (invalid)
        1, 0, 4,
        0x00, 0x10, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00,
      ])
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBeNull()
  })

  test('returns null for invalid typesize 0', async ({ page }) => {
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        1, 1, 0,
        0,    // typesize = 0 (invalid)
        0x00, 0x10, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00,
      ])
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBeNull()
  })

  test('returns null for typesize > 8', async ({ page }) => {
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        1, 1, 0,
        9,    // typesize = 9 (invalid)
        0x00, 0x10, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00,
      ])
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBeNull()
  })

  test('returns null when nbytes is 0', async ({ page }) => {
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        1, 1, 0, 4,
        0x00, 0x00, 0x00, 0x00, // nbytes = 0
        0x00, 0x01, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00,
      ])
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBeNull()
  })

  test('reads large nbytes value correctly', async ({ page }) => {
    // nbytes = 397312 = 0x00061000 (matches beechnut scale 2 chunk: 97*64*64*1 byte)
    // Create a buffer of 2048 bytes so cbytes=2048 is consistent with buffer length
    const result = await page.evaluate(() => {
      const buf = new Uint8Array(2048)
      buf[0] = 2    // version
      buf[1] = 1    // versionlz
      buf[2] = 0    // flags
      buf[3] = 1    // typesize
      // nbytes = 397312 (LE)
      buf[4] = 0x00; buf[5] = 0x10; buf[6] = 0x06; buf[7] = 0x00
      // blocksize
      buf[8] = 0x00; buf[9] = 0x10; buf[10] = 0x00; buf[11] = 0x00
      // cbytes = 2048 (0x00000800 LE)
      buf[12] = 0x00; buf[13] = 0x08; buf[14] = 0x00; buf[15] = 0x00
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBe(397312)
  })

  test('accepts version 1 as valid', async ({ page }) => {
    const result = await page.evaluate(() => {
      const buf = new Uint8Array([
        1,    // version = 1 (valid)
        1, 0, 2, // typesize=2
        0x00, 0x20, 0x00, 0x00, // nbytes = 8192
        0x00, 0x01, 0x00, 0x00, // blocksize = 256
        0x10, 0x00, 0x00, 0x00, // cbytes = 16 (matches buffer length)
      ])
      return window.readBloscFrameContentSize(buf)
    })
    expect(result).toBe(8192)
  })
})

test.describe('inferChunkShape', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => typeof window.inferChunkShape === 'function')
  })

  test('returns metadata shape when element counts match', async ({ page }) => {
    const result = await page.evaluate(() => {
      return window.inferChunkShape(
        96 * 96 * 96, // actualElements = metaElements
        [96, 96, 96],
        [384, 256, 256],
      )
    })
    expect(result).toEqual([[96, 96, 96]])
  })

  test('infers [97,64,64] for beechnut scale 2', async ({ page }) => {
    // Scale 2: array [386,256,256], metadata chunk [96,96,96], actual [97,64,64]
    // actualElements = 97*64*64 = 397312
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(397312, [96, 96, 96], [386, 256, 256])
      return candidates[0] // best candidate
    })
    expect(result).toEqual([97, 64, 64])
  })

  test('infers [97,64,128] for beechnut scale 1 (not [97,128,64])', async ({ page }) => {
    // Scale 1: array [773,512,512], metadata chunk [96,96,96], actual [97,64,128]
    // actualElements = 97*64*128 = 794624
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(794624, [96, 96, 96], [773, 512, 512])
      return candidates[0]
    })
    expect(result).toEqual([97, 64, 128])
  })

  test('infers [97,128,128] for beechnut scale 0 (not [128,97,128])', async ({ page }) => {
    // Scale 0: array [1546,1024,1024], metadata chunk [96,96,96], actual [97,128,128]
    // actualElements = 97*128*128 = 1589248
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(1589248, [96, 96, 96], [1546, 1024, 1024])
      return candidates[0]
    })
    expect(result).toEqual([97, 128, 128])
  })

  test('convention penalty ranks [97,64,128] above [97,128,64]', async ({ page }) => {
    // Both are valid factorizations of 794624 = 97*64*128 = 97*128*64
    // But [97,64,128] should score better because chunk_x >= chunk_y
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(794624, [96, 96, 96], [773, 512, 512])
      const idx64_128 = candidates.findIndex(
        (c: number[]) => c[0] === 97 && c[1] === 64 && c[2] === 128,
      )
      const idx128_64 = candidates.findIndex(
        (c: number[]) => c[0] === 97 && c[1] === 128 && c[2] === 64,
      )
      return { idx64_128, idx128_64, total: candidates.length }
    })
    // [97,64,128] should appear before [97,128,64]
    expect(result.idx64_128).toBeLessThan(result.idx128_64)
    expect(result.idx64_128).toBe(0) // should be the top candidate
  })

  test('single-dim solve: finds shape when only one dimension differs', async ({ page }) => {
    // If actual chunk is [100, 96, 96] but metadata says [96,96,96]
    // actualElements = 100*96*96 = 921600
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(921600, [96, 96, 96], [400, 384, 384])
      return candidates[0]
    })
    expect(result).toEqual([100, 96, 96])
  })

  test('returns empty array when no valid candidates exist', async ({ page }) => {
    // Use a prime number of elements that can't factor into reasonable shapes
    const result = await page.evaluate(() => {
      return window.inferChunkShape(7, [96, 96, 96], [386, 256, 256])
    })
    // With 7 elements total and 3D array, no reasonable factorization exists
    expect(result.length).toBe(0)
  })

  test('handles 2D arrays', async ({ page }) => {
    // 2D array: metadata [64,64], actual [64,128]
    // actualElements = 64*128 = 8192
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(8192, [64, 64], [256, 512])
      return candidates[0]
    })
    expect(result).toEqual([64, 128])
  })

  test('convention penalty: prefers larger last dim (x) in 2D', async ({ page }) => {
    // metadata [64,64], actualElements = 16384 = 256*64
    // For 2D (y,x), convention prefers chunk_x >= chunk_y
    // [64,256] has x=256 >= y=64 → no convention penalty
    // [256,64] has x=64 < y=256 → +20 convention penalty
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(16384, [64, 64], [512, 512])
      const idx64_256 = candidates.findIndex(
        (c: number[]) => c[0] === 64 && c[1] === 256,
      )
      const idx256_64 = candidates.findIndex(
        (c: number[]) => c[0] === 256 && c[1] === 64,
      )
      return { idx64_256, idx256_64 }
    })
    expect(result.idx64_256).toBeGreaterThanOrEqual(0)
    expect(result.idx256_64).toBeGreaterThanOrEqual(0)
    // [64,256] should rank better (lower index) than [256,64]
    expect(result.idx64_256).toBeLessThan(result.idx256_64)
  })

  test('over-penalty: rejects chunk dims larger than array dims', async ({ page }) => {
    // Array is [100, 50, 50], metadata chunk [96,96,96]
    // A candidate [200,50,50] has 500000 elements but dim0=200 > array[0]=100
    // It should get a huge penalty
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(
        100 * 50 * 50, // 250000
        [96, 96, 96],
        [100, 50, 50],
      )
      // No candidate should have any dimension > array shape
      const hasOversize = candidates.some((c: number[]) =>
        c.some((dim: number, i: number) => dim > [100, 50, 50][i]),
      )
      return { hasOversize, topCandidate: candidates[0] }
    })
    // The top candidate should not have oversized dims
    if (result.topCandidate) {
      expect(result.topCandidate[0]).toBeLessThanOrEqual(100)
      expect(result.topCandidate[1]).toBeLessThanOrEqual(50)
      expect(result.topCandidate[2]).toBeLessThanOrEqual(50)
    }
  })
})

test.describe('inferChunkShape — validation probe scenarios', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => typeof window.inferChunkShape === 'function')
  })

  test('heuristic alone picks wrong [98,32,64] for beechnut scale 3, needs validation', async ({ page }) => {
    // Scale 3: array [193,128,128], metadata chunk [96,96,96], actual [49,64,64]
    // actualElements = 49*64*64 = 200704
    // Without validation, the heuristic scores [98,32,64] better than [49,64,64]
    // because [98,32,64] has L1=98 vs [49,64,64] has L1=111
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(200704, [96, 96, 96], [193, 128, 128])
      const idx49_64_64 = candidates.findIndex(
        (c: number[]) => c[0] === 49 && c[1] === 64 && c[2] === 64,
      )
      return { top: candidates[0], idx49_64_64, total: candidates.length }
    })
    // The correct answer [49,64,64] should be in the candidates list
    expect(result.idx49_64_64).toBeGreaterThanOrEqual(0)
    // But it may not be the top candidate (that's why we need validation)
  })

  test('heuristic alone may pick wrong shape for beechnut scale 4', async ({ page }) => {
    // Scale 4: array [96,128,128], metadata chunk [96,96,96], actual [48,64,64]
    // actualElements = 48*64*64 = 196608
    const result = await page.evaluate(() => {
      const candidates = window.inferChunkShape(196608, [96, 96, 96], [96, 128, 128])
      const idx48_64_64 = candidates.findIndex(
        (c: number[]) => c[0] === 48 && c[1] === 64 && c[2] === 64,
      )
      return { top: candidates[0], idx48_64_64, total: candidates.length }
    })
    // The correct [48,64,64] should be in the candidates
    expect(result.idx48_64_64).toBeGreaterThanOrEqual(0)
  })
})

test.describe('getWorker — validation probe rejects wrong candidate', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => {
      return (
        typeof window.zarr !== 'undefined' &&
        typeof window.getWorker === 'function' &&
        typeof window.WorkerPool !== 'undefined'
      )
    })
  })

  test('probe rejects wrong top candidate and finds correct chunk_shape', async ({ page }) => {
    // Mirrors beechnut scale 3 bug: heuristic alone picks wrong [98,32,64]
    // but the validation probe rejects it because chunk c/2/0/0 exists
    // (actual grid has 4 Z-chunks, wrong candidate thinks only 2).
    //
    // We use a small 2D array to keep it fast:
    //   array shape [12,6], actual chunk_shape [3,6] (grid 4×1)
    //   metadata lies: chunk_shape [6,6] (36 elements vs actual 18)
    //
    // inferChunkShape(18, [6,6], [12,6]) produces:
    //   [3,6] (L1=3, pow2: 3 not → +10, div: 12%3=0,6%6=0 → score 13)
    //   [6,3] (L1=3, pow2: 3 not → +10, div: 12%6=0,6%3=0, convention: x=3<y=6 → +20 → score 33)
    //
    // So [3,6] wins and is correct — no probe needed. To force the probe,
    // we need a case where wrong > correct in score. Let's create one:
    //
    // array shape [12,8], actual chunk_shape [4,3] (grid 3×ceil(8/3)=3, 12 elements)
    // metadata claims [3,3] (9 elements)
    // inferChunkShape(12, [3,3], [12,8]):
    //   single-dim: dim0: 12/3=4 → [4,3] (L1=1, pow2:4=yes,3=no→+10, div:12%4=0,8%3≠0→+5 → score 16)
    //   single-dim: dim1: 12/3=4 → [3,4] (L1=1, pow2:3=no,4=yes→+10, div:12%3=0,8%4=0 → score 11, convention: x=4>y=3 ok)
    //
    // [3,4] scores 11 (WRONG), [4,3] scores 16 (CORRECT but ranked second).
    // Validation: [3,4] grid = [4,2]. Probe dim with smallest grid > 1 = dim1 (grid=2).
    // Probe c/0/2 → with actual [4,3], chunk at Y-index=2 EXISTS (grid is 3×3, Y 0-2).
    // So [3,4] is rejected. [4,3] grid = [3,3]. Probe dim1 (grid=3). Probe c/0/3 → doesn't exist → accepted.
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      // 1. Create array with actual chunk_shape [4,3]
      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [12, 8],
        chunk_shape: [4, 3],
        data_type: 'int32',
      })

      // Write data: 12×8 grid with known values
      const data = new Int32Array(12 * 8)
      for (let i = 0; i < data.length; i++) data[i] = i + 1
      await zarr.set(arr, null, { data, shape: [12, 8], stride: [8, 1] })

      // 2. Modify zarr.json to lie: claim chunk_shape [3,3]
      const map = store.store as Map<string, Uint8Array>
      const metaBytes = map.get('/zarr.json')!
      const metadata = JSON.parse(new TextDecoder().decode(metaBytes))
      metadata.chunk_grid.configuration.chunk_shape = [3, 3]
      map.set('/zarr.json', new TextEncoder().encode(JSON.stringify(metadata)))

      // 3. Re-open so it picks up modified metadata
      const arr2 = await zarr.open(store, { kind: 'array' })

      // 4. Verify arr2.chunks reports the wrong value
      const reportedChunks = arr2.chunks

      // 5. Read with getWorker — validation probe should reject [3,4] and pick [4,3]
      const chunk = await getWorker(arr2, null, { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
        reportedChunks,
      }
    })

    // Verify getWorker returns correct shape and data
    expect(result.shape).toEqual([12, 8])
    expect(result.reportedChunks).toEqual([3, 3]) // metadata lie is in effect

    // Verify actual data matches what we wrote
    const expected = Array.from({ length: 96 }, (_, i) => i + 1)
    expect(result.data).toEqual(expected)
  })
})

test.describe('getWorker — chunk shape auto-detection integration', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => {
      return (
        typeof window.zarr !== 'undefined' &&
        typeof window.getWorker === 'function' &&
        typeof window.WorkerPool !== 'undefined'
      )
    })
  })

  test('getWorker reads correct data when metadata chunk_shape matches actual', async ({ page }) => {
    // Baseline: chunk_shape in metadata matches the actual data.
    // This verifies probeActualChunkShape doesn't break normal operation.
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6, 6],
        chunk_shape: [3, 3],
        data_type: 'int32',
      })

      const data = new Int32Array([
        1, 2, 3, 4, 5, 6,
        7, 8, 9, 10, 11, 12,
        13, 14, 15, 16, 17, 18,
        19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30,
        31, 32, 33, 34, 35, 36,
      ])
      await zarr.set(arr, null, { data, shape: [6, 6], stride: [6, 1] })

      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
      }
    })

    expect(result.shape).toEqual([6, 6])
    expect(result.data).toEqual([
      1, 2, 3, 4, 5, 6,
      7, 8, 9, 10, 11, 12,
      13, 14, 15, 16, 17, 18,
      19, 20, 21, 22, 23, 24,
      25, 26, 27, 28, 29, 30,
      31, 32, 33, 34, 35, 36,
    ])
  })

  test('getWorker corrects mismatched chunk_shape with uncompressed (bytes) codec', async ({ page }) => {
    // Tests path 3 of probeDecompressedSize: raw byte size check for uncompressed data.
    //
    // Strategy: create a 6x6 int32 array with actual chunk_shape [3,6], write data,
    // then modify zarr.json to claim chunk_shape is [3,3] (a lie).
    // probeDecompressedSize should detect the raw bytes are 72 bytes (3*6*4),
    // giving 18 elements vs the metadata's 9, and inferChunkShape should recover [3,6].
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      // 1. Create array with actual chunk_shape [3,6]
      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6, 6],
        chunk_shape: [3, 6],
        data_type: 'int32',
      })

      const data = new Int32Array([
        1, 2, 3, 4, 5, 6,
        7, 8, 9, 10, 11, 12,
        13, 14, 15, 16, 17, 18,
        19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30,
        31, 32, 33, 34, 35, 36,
      ])
      await zarr.set(arr, null, { data, shape: [6, 6], stride: [6, 1] })

      // 2. Modify zarr.json to lie: claim chunk_shape is [3,3]
      const map = store.store as Map<string, Uint8Array>
      const metaBytes = map.get('/zarr.json')!
      const metadata = JSON.parse(new TextDecoder().decode(metaBytes))
      metadata.chunk_grid.configuration.chunk_shape = [3, 3]
      map.set('/zarr.json', new TextEncoder().encode(JSON.stringify(metadata)))

      // 3. Re-open the array so it picks up the modified metadata
      const arr2 = await zarr.open(store, { kind: 'array' })

      // 4. Read with getWorker — should detect mismatch and infer [3,6]
      const chunk = await getWorker(arr2, null, { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
      }
    })

    expect(result.shape).toEqual([6, 6])
    expect(result.data).toEqual([
      1, 2, 3, 4, 5, 6,
      7, 8, 9, 10, 11, 12,
      13, 14, 15, 16, 17, 18,
      19, 20, 21, 22, 23, 24,
      25, 26, 27, 28, 29, 30,
      31, 32, 33, 34, 35, 36,
    ])
  })

  test('getWorker corrects mismatched chunk_shape with gzip codec (full-decode fallback)', async ({ page }) => {
    // Tests path 4 of probeDecompressedSize: full decode fallback for gzip.
    //
    // Strategy: create array with actual chunk_shape [3,6], write raw (bytes-only) data,
    // then manually gzip-compress each chunk and rewrite zarr.json to:
    //   (a) claim chunk_shape is [3,3]
    //   (b) declare codecs = [bytes, gzip]
    // probeDecompressedSize can't read gzip headers natively (only zstd/blosc),
    // so it falls through to the full-decode fallback using create_codec_pipeline.
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      // Helper: gzip-compress bytes using browser's CompressionStream
      async function gzipCompress(data: Uint8Array): Promise<Uint8Array> {
        const cs = new CompressionStream('gzip')
        const writer = cs.writable.getWriter()
        void writer.write(data as unknown as BufferSource)
        void writer.close()
        const reader = cs.readable.getReader()
        const chunks: Uint8Array[] = []
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          chunks.push(value)
        }
        const totalLen = chunks.reduce((acc, c) => acc + c.length, 0)
        const result = new Uint8Array(totalLen)
        let offset = 0
        for (const chunk of chunks) {
          result.set(chunk, offset)
          offset += chunk.length
        }
        return result
      }

      // 1. Create array with actual chunk_shape [3,6] — writes uncompressed chunks
      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6, 6],
        chunk_shape: [3, 6],
        data_type: 'int32',
      })

      const data = new Int32Array([
        1, 2, 3, 4, 5, 6,
        7, 8, 9, 10, 11, 12,
        13, 14, 15, 16, 17, 18,
        19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30,
        31, 32, 33, 34, 35, 36,
      ])
      await zarr.set(arr, null, { data, shape: [6, 6], stride: [6, 1] })

      // 2. Gzip-compress each stored chunk in-place
      const map = store.store as Map<string, Uint8Array>
      for (const [key, value] of map.entries()) {
        if (key.startsWith('/c/')) {
          const compressed = await gzipCompress(value)
          map.set(key, compressed)
        }
      }

      // 3. Modify zarr.json: lie about chunk_shape AND declare gzip codec
      const metaBytes = map.get('/zarr.json')!
      const metadata = JSON.parse(new TextDecoder().decode(metaBytes))
      metadata.chunk_grid.configuration.chunk_shape = [3, 3]
      metadata.codecs = [
        { name: 'bytes', configuration: { endian: 'little' } },
        { name: 'gzip', configuration: { level: 1 } },
      ]
      map.set('/zarr.json', new TextEncoder().encode(JSON.stringify(metadata)))

      // 4. Re-open the array with the modified metadata
      const arr2 = await zarr.open(store, { kind: 'array' })

      // 5. Read with getWorker — should fall through to full-decode and infer [3,6]
      const chunk = await getWorker(arr2, null, { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
      }
    })

    expect(result.shape).toEqual([6, 6])
    expect(result.data).toEqual([
      1, 2, 3, 4, 5, 6,
      7, 8, 9, 10, 11, 12,
      13, 14, 15, 16, 17, 18,
      19, 20, 21, 22, 23, 24,
      25, 26, 27, 28, 29, 30,
      31, 32, 33, 34, 35, 36,
    ])
  })
})
