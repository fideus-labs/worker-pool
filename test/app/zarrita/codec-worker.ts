/**
 * Web Worker that handles encode/decode operations for zarrita chunks.
 *
 * Message protocol:
 *   decode: { type: 'decode', id, bytes: ArrayBuffer, dtype, shape, delay? }
 *           → { type: 'decoded', id, data: ArrayBuffer, shape, stride }
 *
 *   encode: { type: 'encode', id, data: ArrayBuffer, dtype, shape, delay? }
 *           → { type: 'encoded', id, bytes: ArrayBuffer }
 */

const ctx = self as unknown as DedicatedWorkerGlobalScope

// Map dtype string to TypedArray constructor
function get_ctr(dtype: string): {
  new (buf: ArrayBuffer, offset?: number, len?: number): ArrayBufferView & ArrayLike<number>
  BYTES_PER_ELEMENT: number
} {
  const map: Record<string, unknown> = {
    int8: Int8Array,
    int16: Int16Array,
    int32: Int32Array,
    uint8: Uint8Array,
    uint16: Uint16Array,
    uint32: Uint32Array,
    float32: Float32Array,
    float64: Float64Array,
  }
  const ctr = map[dtype]
  if (!ctr) throw new Error(`Unknown data type: ${dtype}`)
  return ctr as ReturnType<typeof get_ctr>
}

// Compute C-order strides
function get_strides(shape: number[]): number[] {
  const rank = shape.length
  let step = 1
  const stride = new Array<number>(rank)
  for (let i = rank - 1; i >= 0; i--) {
    stride[i] = step
    step *= shape[i]
  }
  return stride
}

function maybeDelay(delay: number | undefined): Promise<void> {
  if (delay && delay > 0) {
    return new Promise((resolve) => setTimeout(resolve, delay))
  }
  return Promise.resolve()
}

ctx.addEventListener('message', async (event: MessageEvent) => {
  const msg = event.data

  if (msg.type === 'decode') {
    await maybeDelay(msg.delay)

    // "Decode": reinterpret raw bytes as a TypedArray.
    // In a real codec this would decompress, byteswap, etc.
    const Ctr = get_ctr(msg.dtype)
    const bytesCopy = new ArrayBuffer(msg.bytes.byteLength)
    new Uint8Array(bytesCopy).set(new Uint8Array(msg.bytes))
    const data = new Ctr(bytesCopy, 0, bytesCopy.byteLength / Ctr.BYTES_PER_ELEMENT)
    const stride = get_strides(msg.shape)

    // Transfer the buffer back
    ctx.postMessage(
      {
        type: 'decoded',
        id: msg.id,
        data: data.buffer,
        shape: msg.shape,
        stride,
      },
      [data.buffer],
    )
  } else if (msg.type === 'encode') {
    await maybeDelay(msg.delay)

    // "Encode": reinterpret TypedArray as raw bytes.
    const bytesCopy = new ArrayBuffer(msg.data.byteLength)
    new Uint8Array(bytesCopy).set(new Uint8Array(msg.data))

    ctx.postMessage(
      {
        type: 'encoded',
        id: msg.id,
        bytes: bytesCopy,
      },
      [bytesCopy],
    )
  }
})
