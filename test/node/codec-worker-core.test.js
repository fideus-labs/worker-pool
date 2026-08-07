/**
 * handleCodecMessage — the protocol both worker entries share.
 *
 * Driven directly rather than through a worker, so the reply shapes and the
 * failure mapping are checked without a thread in the way.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { handleCodecMessage } from '../../fizarrita/dist/index.js'

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
  // No init for this metaId, and no inline meta to fall back on.
  const cases = [
    ['decode', 'decoded'],
    ['encode', 'encoded'],
    ['decode_into', 'decode_into_ok'],
  ]
  for (const [requestType, responseType] of cases) {
    const reply = await handleCodecMessage({
      type: requestType,
      id: 8,
      metaId: 999_999,
      bytes: new ArrayBuffer(16),
      data: new ArrayBuffer(16),
    })
    assert.equal(reply.response.type, responseType, requestType)
    assert.equal(reply.response.id, 8)
    assert.equal(typeof reply.response.error, 'string', requestType)
    assert.ok(reply.response.error.length > 0, requestType)
    assert.deepEqual(reply.transfer, [])
  }
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
})

test('the node worker entry refuses to run on the main thread', async () => {
  await assert.rejects(
    import('../../fizarrita/dist/codec-worker-node.js'),
    /must be run as a worker thread/,
  )
})
