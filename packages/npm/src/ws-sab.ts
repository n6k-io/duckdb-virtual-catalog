// Lock-step SAB protocol between duckdb-worker (requester) and ws-worker (responder); single-shot + streaming with flow control.

export const WS_SAB_SIZE = 1 << 20; // 1 MB — handles typical INSERT/RPC Arrow payloads.

export const WS_FLAG = 0;
export const WS_OP = 1;
export const WS_BODY_LEN = 2;
export const WS_RESP_LEN = 3;

export const WS_IDLE = 0;
export const WS_REQ_READY = 1;
export const WS_RESP_READY = 2;
export const WS_ERR = 3;
export const WS_CHUNK_READY = 4;
export const WS_CHUNK_NEXT = 5;
export const WS_STREAM_END = 6;

export const WS_CONTROL_BYTES = 64;
export const WS_BODY_OFFSET = WS_CONTROL_BYTES;
export const WS_BODY_CAPACITY = 1 << 19;
export const WS_RESP_OFFSET = WS_BODY_OFFSET + WS_BODY_CAPACITY;
export const WS_RESP_CAPACITY = WS_SAB_SIZE - WS_RESP_OFFSET;

export function wsSabViews(sab: SharedArrayBuffer) {
  return {
    ctrl: new Int32Array(sab, 0, 16),
    body: new Uint8Array(sab, WS_BODY_OFFSET, WS_BODY_CAPACITY),
    resp: new Uint8Array(sab, WS_RESP_OFFSET, WS_RESP_CAPACITY),
  };
}
