/**
 * handleCodecMessage — the protocol both worker entries share.
 *
 * Driven directly rather than through a worker, so the reply shapes and the
 * failure mapping are checked without a thread in the way.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { registry } from 'zarrita'

import { getMetaId, handleCodecMessage } from '../../fizarrita/dist/index.js'

const BYTES_META = {
  data_type: 'int32',
  chunk_shape: [2, 2],
  codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
}

/** metaIds are process-global in the worker; keep each test on its own. */
let nextMetaId = 1000
const freshMetaId = () => nextMetaId++

test('init registers a pipeline and acknowledges', async () => {
  const metaId = freshMetaId()
  const reply = await handleCodecMessage({
    type: 'init',
    id: 1,
    metaId,
    meta: BYTES_META,
  })
  assert.deepEqual(reply.response, { type: 'init_ok', id: 1 })
  assert.deepEqual(reply.transfer, [])
})

test('encode then decode round-trips through a registered metaId', async () => {
  const metaId = freshMetaId()
  await handleCodecMessage({ type: 'init', id: 1, metaId, meta: BYTES_META })

  const values = Int32Array.from([1, 2, 3, 4])
  const encoded = await handleCodecMessage({
    type: 'encode',
    id: 2,
    data: values.buffer.slice(0),
    metaId,
  })
  assert.equal(encoded.response.type, 'encoded')
  assert.equal(encoded.transfer.length, 1, 'encoded bytes are handed over')
  assert.equal(encoded.transfer[0], encoded.response.bytes)

  const decoded = await handleCodecMessage({
    type: 'decode',
    id: 3,
    bytes: encoded.response.bytes,
    metaId,
  })
  assert.equal(decoded.response.type, 'decoded')
  assert.deepEqual(decoded.response.shape, [2, 2])
  assert.deepEqual(decoded.response.stride, [2, 1])
  assert.deepEqual(Array.from(new Int32Array(decoded.response.data)), [1, 2, 3, 4])
})

test('a full meta object still works without a prior init', async () => {
  // Legacy callers send `meta` inline instead of referencing a registered id.
  const values = Int32Array.from([9, 8, 7, 6])
  const encoded = await handleCodecMessage({
    type: 'encode',
    id: 4,
    data: values.buffer.slice(0),
    meta: BYTES_META,
  })
  assert.equal(encoded.response.type, 'encoded')

  const decoded = await handleCodecMessage({
    type: 'decode',
    id: 5,
    bytes: encoded.response.bytes,
    meta: BYTES_META,
  })
  assert.deepEqual(Array.from(new Int32Array(decoded.response.data)), [9, 8, 7, 6])
})

test('actualChunkShape narrows an edge chunk', async () => {
  const metaId = freshMetaId()
  await handleCodecMessage({ type: 'init', id: 1, metaId, meta: BYTES_META })

  // A 2x2 chunk decoded as the 1x2 edge it really is: the padded tail is dropped.
  const values = Int32Array.from([1, 2, 3, 4])
  const decoded = await handleCodecMessage({
    type: 'decode',
    id: 6,
    bytes: values.buffer.slice(0),
    metaId,
    actualChunkShape: [1, 2],
  })
  assert.deepEqual(decoded.response.shape, [1, 2])
  assert.deepEqual(decoded.response.stride, [2, 1])
  assert.deepEqual(Array.from(new Int32Array(decoded.response.data)), [1, 2])
})

test('an unrecognised message type produces no reply', async () => {
  assert.equal(await handleCodecMessage({ type: 'nonsense', id: 7 }), null)
})

test('a failure comes back as the request’s own response type', async () => {
  // No init for this metaId, and no inline meta to fall back on. The error has
  // to name that cause — a request for codec metadata the worker never got —
  // rather than whatever the pipeline builder happens to trip over first.
  const cases = [
    ['decode', 'decoded', /No codec metadata for metaId 999999/],
    ['encode', 'encoded', /No codec metadata for metaId 999999/],
    ['decode_into', 'decode_into_ok', /No pipeline for metaId 999999/],
  ]
  for (const [requestType, responseType, expectedError] of cases) {
    const reply = await handleCodecMessage({
      type: requestType,
      id: 8,
      metaId: 999_999,
      bytes: new ArrayBuffer(16),
      data: new ArrayBuffer(16),
    })
    assert.equal(reply.response.type, responseType, requestType)
    assert.equal(reply.response.id, 8)
    assert.match(reply.response.error, expectedError, requestType)
    assert.deepEqual(reply.transfer, [])
  }
})

test('a request with neither metaId nor meta says so', async () => {
  const reply = await handleCodecMessage({
    type: 'decode',
    id: 11,
    bytes: new ArrayBuffer(16),
  })
  assert.equal(reply.response.type, 'decoded')
  assert.match(reply.response.error, /Send an 'init' message first/)
})

test('a bad codec surfaces as an error reply, not a rejection', async () => {
  // Pipelines build lazily, so `init` accepts this and the codec is only
  // resolved on first use — the failure has to survive the decode path.
  const metaId = freshMetaId()
  const init = await handleCodecMessage({
    type: 'init',
    id: 9,
    metaId,
    meta: { ...BYTES_META, codecs: [{ name: 'no-such-codec', configuration: {} }] },
  })
  assert.deepEqual(init.response, { type: 'init_ok', id: 9 })

  const reply = await handleCodecMessage({
    type: 'decode',
    id: 10,
    bytes: new ArrayBuffer(16),
    metaId,
  })
  assert.equal(reply.response.type, 'decoded')
  assert.equal(reply.response.id, 10)
  assert.match(reply.response.error, /Unknown codec: no-such-codec/)
  assert.equal(reply.response.errorInfo.tag, 'UnknownCodecError')
  assert.equal(reply.response.errorInfo.codec, 'no-such-codec')
})

test('a codec that throws is reported as zarrita’s CodecPipelineError', async () => {
  const metaId = freshMetaId()
  await handleCodecMessage({
    type: 'init',
    id: 12,
    metaId,
    meta: {
      ...BYTES_META,
      codecs: [...BYTES_META.codecs, { name: 'zstd', configuration: { level: 1 } }],
    },
  })

  const reply = await handleCodecMessage({
    type: 'decode',
    id: 13,
    bytes: Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]).buffer,
    metaId,
  })
  assert.equal(reply.response.type, 'decoded')
  const { errorInfo } = reply.response
  assert.equal(errorInfo.tag, 'CodecPipelineError')
  assert.equal(errorInfo.codec, 'zstd')
  assert.equal(errorInfo.direction, 'decode')
  assert.equal(typeof errorInfo.cause, 'string')
  // The message names the codec and says what went wrong inside it.
  assert.match(reply.response.error, /decode chunk via codec "zstd": ./)
})

test('encode honours the stride the chunk is laid out in', async () => {
  const metaId = freshMetaId()
  await handleCodecMessage({
    type: 'init',
    id: 14,
    metaId,
    meta: {
      ...BYTES_META,
      codecs: [
        { name: 'transpose', configuration: { order: [1, 0] } },
        ...BYTES_META.codecs,
      ],
    },
  })

  // The 2x2 matrix [[1, 2], [3, 4]], once in C order and once already in the
  // transposed (column-major) layout the codec stores: both must encode to
  // the same bytes.
  const encode = async (values, stride) =>
    new Int32Array(
      (
        await handleCodecMessage({
          type: 'encode',
          id: 15,
          data: Int32Array.from(values).buffer,
          metaId,
          stride,
        })
      ).response.bytes,
    )
  const fromC = await encode([1, 2, 3, 4], undefined)
  const fromF = await encode([1, 3, 2, 4], [1, 2])
  assert.deepEqual(Array.from(fromC), [1, 3, 2, 4])
  assert.deepEqual(Array.from(fromF), Array.from(fromC))
})

test('codecs are configured with the array\u2019s typed fill value, as by zarrita', async () => {
  // A codec that records the metadata it was built with.
  const seen = []
  registry.set('fizarrita-test-meta-probe', async () => ({
    fromConfig(_config, meta) {
      seen.push(meta)
      return {
        kind: 'bytes_to_bytes',
        encode: (bytes) => bytes,
        decode: (bytes) => bytes,
      }
    },
  }))
  const metaId = freshMetaId()
  await handleCodecMessage({
    type: 'init',
    id: 16,
    metaId,
    meta: {
      data_type: 'int64',
      chunk_shape: [2],
      codecs: [
        { name: 'bytes', configuration: { endian: 'little' } },
        { name: 'fizarrita-test-meta-probe', configuration: {} },
      ],
      fill_value: 5n,
    },
  })
  const reply = await handleCodecMessage({
    type: 'decode',
    id: 17,
    bytes: new BigInt64Array([1n, 2n]).buffer,
    metaId,
  })
  assert.equal(reply.response.error, undefined)
  assert.deepEqual(Array.from(new BigInt64Array(reply.response.data)), [1n, 2n])
  assert.equal(seen.length, 1)
  assert.equal(seen[0].dataType, 'int64')
  assert.equal(seen[0].fillValue, 5n)
  assert.deepEqual(seen[0].shape, [2])
})

test('metaIds tell fill values apart, bigint and non-finite ones included', async () => {
  const base = { data_type: 'int64', chunk_shape: [2], codecs: [] }
  assert.equal(getMetaId({ ...base, fill_value: 1n }), getMetaId({ ...base, fill_value: 1n }))
  assert.notEqual(getMetaId({ ...base, fill_value: 1n }), getMetaId({ ...base, fill_value: 2n }))
  const floats = { data_type: 'float32', chunk_shape: [2], codecs: [] }
  assert.notEqual(getMetaId({ ...floats, fill_value: NaN }), getMetaId({ ...floats, fill_value: null }))
  assert.notEqual(getMetaId({ ...floats, fill_value: Infinity }), getMetaId({ ...floats, fill_value: -Infinity }))
})

test('the node worker entry refuses to run on the main thread', async () => {
  await assert.rejects(
    import('../../fizarrita/dist/codec-worker-node.js'),
    /must be run as a worker thread/,
  )
})
