import {
  FLAG,
  URL_LEN,
  RESP_LEN,
  HTTP_STATUS,
  CHUNK_OFFSET,
  CHUNK_LEN,
  BODY_LEN,
  IDLE,
  REQUEST,
  RESPONSE_READY,
  ERROR,
  CHUNK_REQUEST,
  CHUNK_READY,
  BODY_CHUNK_REQUEST,
  BODY_CHUNK_READY,
} from "./protocol";

export function encodeUrlIntoSABUrlBuffer(
  ctrl: Int32Array,
  urlBuf: Uint8Array,
  url: string,
  enc: TextEncoder,
): number {
  const encoded = enc.encode(url);
  urlBuf.set(encoded);
  Atomics.store(ctrl, URL_LEN, encoded.length);
  return encoded.length;
}

export function encodeBodyIntoSABUrlBufferAfterUrl(
  ctrl: Int32Array,
  urlBuf: Uint8Array,
  body: string,
  urlOffset: number,
  enc: TextEncoder,
): void {
  const encoded = enc.encode(body);
  urlBuf.set(encoded, urlOffset);
  Atomics.store(ctrl, BODY_LEN, encoded.length);
}

export function storeZeroBodyLengthForGetRequest(ctrl: Int32Array): void {
  Atomics.store(ctrl, BODY_LEN, 0);
}

export function storeNegativeOneBodyLengthForDeleteRequest(
  ctrl: Int32Array,
): void {
  Atomics.store(ctrl, BODY_LEN, -1);
}

export function storeNegativeBodyLengthForBinaryUpload(
  ctrl: Int32Array,
  bodyLen: number,
): void {
  Atomics.store(ctrl, BODY_LEN, -bodyLen);
}

export function clearStoredBodyLength(ctrl: Int32Array): void {
  Atomics.store(ctrl, BODY_LEN, 0);
}

export function dispatchSABRequestAndBlockUntilFetchWorkerResponds(
  ctrl: Int32Array,
): void {
  Atomics.store(ctrl, FLAG, REQUEST);
  Atomics.notify(ctrl, FLAG);
  Atomics.wait(ctrl, FLAG, REQUEST);
}

export function loadResponseFlagLengthAndStatusFromSAB(ctrl: Int32Array): {
  flag: number;
  respLen: number;
  status: number;
} {
  return {
    flag: Atomics.load(ctrl, FLAG),
    respLen: Atomics.load(ctrl, RESP_LEN),
    status: Atomics.load(ctrl, HTTP_STATUS),
  };
}

export function returnErrorStringIfResponseFailedAndReleaseChannel(
  ctrl: Int32Array,
  dataBuf: Uint8Array,
  dec: TextDecoder,
): string | null {
  const flag = Atomics.load(ctrl, FLAG);
  const respLen = Atomics.load(ctrl, RESP_LEN);
  const status = Atomics.load(ctrl, HTTP_STATUS);
  if (flag === ERROR) {
    const body = dec.decode(dataBuf.slice(0, respLen));
    Atomics.store(ctrl, FLAG, IDLE);
    // status==0 = transport failure; >0 = HTTP non-2xx — encoded so C++ can distinguish.
    if (status > 0) {
      return "HTTP_ERROR:" + status + ":" + body;
    }
    return "FETCH_ERROR: " + body;
  }
  if (status < 200 || status >= 300) {
    const body = dec.decode(dataBuf.slice(0, respLen));
    Atomics.store(ctrl, FLAG, IDLE);
    return "HTTP_ERROR:" + status + ":" + body;
  }
  return null;
}

export function decodeResponseTextFromSABDataBuffer(
  ctrl: Int32Array,
  dataBuf: Uint8Array,
  dec: TextDecoder,
): string {
  const respLen = Atomics.load(ctrl, RESP_LEN);
  return dec.decode(dataBuf.slice(0, respLen));
}

export function storeFlagIdleToReleaseSABChannel(ctrl: Int32Array): void {
  Atomics.store(ctrl, FLAG, IDLE);
  Atomics.notify(ctrl, FLAG);
}

export function requestNextResponseChunkFromFetchWorkerBlocking(
  ctrl: Int32Array,
  offset: number,
): number {
  Atomics.store(ctrl, CHUNK_OFFSET, offset);
  Atomics.store(ctrl, FLAG, CHUNK_REQUEST);
  Atomics.notify(ctrl, FLAG);
  Atomics.wait(ctrl, FLAG, CHUNK_REQUEST);
  return Atomics.load(ctrl, CHUNK_LEN);
}

export function readAllResponseChunksFromSABAsText(
  ctrl: Int32Array,
  dataBuf: Uint8Array,
  dec: TextDecoder,
): string {
  const totalLen = Atomics.load(ctrl, RESP_LEN);
  const chunks: string[] = [];
  let offset = 0;
  while (offset < totalLen) {
    const chunkLen = requestNextResponseChunkFromFetchWorkerBlocking(
      ctrl,
      offset,
    );
    chunks.push(dec.decode(dataBuf.slice(0, chunkLen)));
    offset += chunkLen;
  }
  Atomics.store(ctrl, FLAG, IDLE);
  Atomics.notify(ctrl, FLAG);
  return chunks.join("");
}

export function copyAllResponseChunksFromSABIntoWASMHeap(
  ctrl: Int32Array,
  dataBuf: Uint8Array,
  ptr: number,
  heap: Uint8Array,
): number {
  const totalLen = Atomics.load(ctrl, RESP_LEN);
  let offset = 0;
  while (offset < totalLen) {
    const chunkLen = requestNextResponseChunkFromFetchWorkerBlocking(
      ctrl,
      offset,
    );
    heap.set(dataBuf.subarray(0, chunkLen), ptr + offset);
    offset += chunkLen;
  }
  Atomics.store(ctrl, FLAG, IDLE);
  Atomics.notify(ctrl, FLAG);
  return totalLen;
}

export function encodeContentTypeHeaderIntoSABDataBuffer(
  ctrl: Int32Array,
  dataBuf: Uint8Array,
  boundary: string,
  enc: TextEncoder,
): void {
  const ctHeader = enc.encode("multipart/mixed; boundary=" + boundary);
  dataBuf.set(ctHeader);
  Atomics.store(ctrl, CHUNK_LEN, ctHeader.length);
}

export function waitForFetchWorkerToRequestBodyChunk(ctrl: Int32Array): number {
  Atomics.wait(ctrl, FLAG, REQUEST);
  return Atomics.load(ctrl, FLAG);
}

export function writeBodyChunkFromHeapIntoSABAndSignalReady(
  ctrl: Int32Array,
  dataBuf: Uint8Array,
  heap: Uint8Array,
  bodyPtr: number,
  offset: number,
  chunkSize: number,
  isLast: boolean,
): void {
  dataBuf.set(heap.subarray(bodyPtr + offset, bodyPtr + offset + chunkSize));
  Atomics.store(ctrl, CHUNK_LEN, chunkSize);
  Atomics.store(ctrl, CHUNK_OFFSET, isLast ? 1 : 0);
  Atomics.store(ctrl, FLAG, BODY_CHUNK_READY);
  Atomics.notify(ctrl, FLAG);
}

export function blockUntilFetchWorkerRespondsAfterBodyUpload(
  ctrl: Int32Array,
): void {
  Atomics.wait(ctrl, FLAG, BODY_CHUNK_READY);
}

export function signalDuckDBWorkerConnectionSucceeded(ctrl: Int32Array): void {
  Atomics.store(ctrl, BODY_LEN, 1);
  Atomics.notify(ctrl, BODY_LEN);
}

export function signalDuckDBWorkerConnectionFailed(ctrl: Int32Array): void {
  Atomics.store(ctrl, BODY_LEN, 2);
  Atomics.notify(ctrl, BODY_LEN);
}

export function blockUntilConnectionSignalAndReturn(ctrl: Int32Array): number {
  Atomics.wait(ctrl, BODY_LEN, 0);
  const signal = Atomics.load(ctrl, BODY_LEN);
  Atomics.store(ctrl, BODY_LEN, 0);
  return signal;
}

export function blockUntilRequestArrivesFromDuckDBWorker(
  ctrl: Int32Array,
): void {
  while (true) {
    const c = Atomics.load(ctrl, FLAG);
    if (c === REQUEST) return;
    Atomics.wait(ctrl, FLAG, c);
  }
}

export function loadRequestUrlLengthAndBodyLengthFromSAB(ctrl: Int32Array): {
  urlLen: number;
  bodyLen: number;
} {
  return {
    urlLen: Atomics.load(ctrl, URL_LEN),
    bodyLen: Atomics.load(ctrl, BODY_LEN),
  };
}

export function storeResponseAndSignalReadyToSABChannel(
  ctrl: Int32Array,
  len: number,
  status: number,
): void {
  Atomics.store(ctrl, RESP_LEN, len);
  Atomics.store(ctrl, HTTP_STATUS, status);
  Atomics.store(ctrl, FLAG, RESPONSE_READY);
  Atomics.notify(ctrl, FLAG);
}

export function storeErrorAndSignalToSABChannel(
  ctrl: Int32Array,
  len: number,
  status: number,
): void {
  Atomics.store(ctrl, RESP_LEN, len);
  Atomics.store(ctrl, HTTP_STATUS, status);
  Atomics.store(ctrl, FLAG, ERROR);
  Atomics.notify(ctrl, FLAG);
}

export function loadChunkOffsetRequestedByDuckDBWorker(
  ctrl: Int32Array,
): number {
  return Atomics.load(ctrl, CHUNK_OFFSET);
}

export function storeChunkAndSignalReadyToSABChannel(
  ctrl: Int32Array,
  len: number,
): void {
  Atomics.store(ctrl, CHUNK_LEN, len);
  Atomics.store(ctrl, FLAG, CHUNK_READY);
  Atomics.notify(ctrl, FLAG);
}

export function blockUntilDuckDBWorkerRequestsChunkOrFinishes(
  ctrl: Int32Array,
): number {
  while (true) {
    const c = Atomics.load(ctrl, FLAG);
    if (c === IDLE) return IDLE;
    if (c === CHUNK_REQUEST) return CHUNK_REQUEST;
    if (c === REQUEST) return REQUEST;
    Atomics.wait(ctrl, FLAG, c);
  }
}

export function loadContentTypeLengthAlreadyStoredInSAB(
  ctrl: Int32Array,
): number {
  return Atomics.load(ctrl, CHUNK_LEN);
}

export function signalBodyChunkRequestAndBlockUntilChunkArrives(
  ctrl: Int32Array,
): number {
  Atomics.store(ctrl, FLAG, BODY_CHUNK_REQUEST);
  Atomics.notify(ctrl, FLAG);
  Atomics.wait(ctrl, FLAG, BODY_CHUNK_REQUEST);
  return Atomics.load(ctrl, CHUNK_LEN);
}

import {
  WS_FLAG,
  WS_OP,
  WS_BODY_LEN,
  WS_RESP_LEN,
  WS_IDLE,
  WS_REQ_READY,
  WS_ERR,
  WS_CHUNK_NEXT,
  WS_STREAM_END,
  wsSabViews,
} from "./ws-sab";

// Single-shot request; bytes stay in ws-SAB resp until a reader helper consumes them.
export function wsReqTextBody(
  wsSab: SharedArrayBuffer,
  op: number,
  bodyBytes: Uint8Array,
  dec: TextDecoder,
): string {
  const v = wsSabViews(wsSab);
  if (bodyBytes.byteLength > v.body.byteLength) {
    return "WS_ERROR:body too large for ws-SAB";
  }
  v.body.set(bodyBytes, 0);
  Atomics.store(v.ctrl, WS_OP, op);
  Atomics.store(v.ctrl, WS_BODY_LEN, bodyBytes.byteLength);
  Atomics.store(v.ctrl, WS_RESP_LEN, 0);
  Atomics.store(v.ctrl, WS_FLAG, WS_REQ_READY);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
  let flag = Atomics.load(v.ctrl, WS_FLAG);
  while (flag === WS_REQ_READY) {
    Atomics.wait(v.ctrl, WS_FLAG, WS_REQ_READY);
    flag = Atomics.load(v.ctrl, WS_FLAG);
  }
  const respLen = Atomics.load(v.ctrl, WS_RESP_LEN);
  if (flag === WS_ERR) {
    const errText = dec.decode(v.resp.slice(0, respLen));
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return errText.startsWith("WS_ERROR:") ? errText : "WS_ERROR:" + errText;
  }
  return "OK:" + respLen;
}

export function wsReqBinaryBody(
  wsSab: SharedArrayBuffer,
  op: number,
  headerBytes: Uint8Array,
  bodyPtr: number,
  bodyLen: number,
  heap: Uint8Array,
  dec: TextDecoder,
): string {
  const v = wsSabViews(wsSab);
  const total = headerBytes.byteLength + bodyLen;
  if (total > v.body.byteLength) {
    return "WS_ERROR:body too large for ws-SAB";
  }
  v.body.set(headerBytes, 0);
  if (bodyLen > 0) {
    v.body.set(
      heap.subarray(bodyPtr, bodyPtr + bodyLen),
      headerBytes.byteLength,
    );
  }
  Atomics.store(v.ctrl, WS_OP, op);
  Atomics.store(v.ctrl, WS_BODY_LEN, total);
  Atomics.store(v.ctrl, WS_RESP_LEN, 0);
  Atomics.store(v.ctrl, WS_FLAG, WS_REQ_READY);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
  let flag = Atomics.load(v.ctrl, WS_FLAG);
  while (flag === WS_REQ_READY) {
    Atomics.wait(v.ctrl, WS_FLAG, WS_REQ_READY);
    flag = Atomics.load(v.ctrl, WS_FLAG);
  }
  const respLen = Atomics.load(v.ctrl, WS_RESP_LEN);
  if (flag === WS_ERR) {
    const errText = dec.decode(v.resp.slice(0, respLen));
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return errText.startsWith("WS_ERROR:") ? errText : "WS_ERROR:" + errText;
  }
  return "OK:" + respLen;
}

export function wsReadResponseText(
  wsSab: SharedArrayBuffer,
  dec: TextDecoder,
): string {
  const v = wsSabViews(wsSab);
  const respLen = Atomics.load(v.ctrl, WS_RESP_LEN);
  const text = dec.decode(v.resp.slice(0, respLen));
  Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
  return text;
}

export function wsReadResponseIntoHeap(
  wsSab: SharedArrayBuffer,
  ptr: number,
  heap: Uint8Array,
): number {
  const v = wsSabViews(wsSab);
  const respLen = Atomics.load(v.ctrl, WS_RESP_LEN);
  heap.set(v.resp.subarray(0, respLen), ptr);
  Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
  return respLen;
}

// Streaming kickoff; returns "CHUNK:N", "END", or "WS_ERROR:msg".
export function wsStartStreamAndWaitFirstChunk(
  wsSab: SharedArrayBuffer,
  op: number,
  bodyBytes: Uint8Array,
  dec: TextDecoder,
): string {
  const v = wsSabViews(wsSab);
  if (bodyBytes.byteLength > v.body.byteLength) {
    return "WS_ERROR:body too large for ws-SAB";
  }
  v.body.set(bodyBytes, 0);
  Atomics.store(v.ctrl, WS_OP, op);
  Atomics.store(v.ctrl, WS_BODY_LEN, bodyBytes.byteLength);
  Atomics.store(v.ctrl, WS_RESP_LEN, 0);
  Atomics.store(v.ctrl, WS_FLAG, WS_REQ_READY);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
  let flag = Atomics.load(v.ctrl, WS_FLAG);
  while (flag === WS_REQ_READY) {
    Atomics.wait(v.ctrl, WS_FLAG, WS_REQ_READY);
    flag = Atomics.load(v.ctrl, WS_FLAG);
  }
  if (flag === WS_ERR) {
    const errText = dec.decode(
      v.resp.slice(0, Atomics.load(v.ctrl, WS_RESP_LEN)),
    );
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return errText.startsWith("WS_ERROR:") ? errText : "WS_ERROR:" + errText;
  }
  if (flag === WS_STREAM_END) {
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return "END";
  }
  return "CHUNK:" + Atomics.load(v.ctrl, WS_RESP_LEN);
}

// Binary-body streaming kickoff (RPC_TABLE Arrow IPC); returns "CHUNK:N", "END", or "WS_ERROR:msg".
export function wsStartStreamBinaryAndWaitFirstChunk(
  wsSab: SharedArrayBuffer,
  op: number,
  headerBytes: Uint8Array,
  bodyPtr: number,
  bodyLen: number,
  heap: Uint8Array,
  dec: TextDecoder,
): string {
  const v = wsSabViews(wsSab);
  const total = headerBytes.byteLength + bodyLen;
  if (total > v.body.byteLength) {
    return "WS_ERROR:body too large for ws-SAB";
  }
  v.body.set(headerBytes, 0);
  if (bodyLen > 0) {
    v.body.set(
      heap.subarray(bodyPtr, bodyPtr + bodyLen),
      headerBytes.byteLength,
    );
  }
  Atomics.store(v.ctrl, WS_OP, op);
  Atomics.store(v.ctrl, WS_BODY_LEN, total);
  Atomics.store(v.ctrl, WS_RESP_LEN, 0);
  Atomics.store(v.ctrl, WS_FLAG, WS_REQ_READY);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
  let flag = Atomics.load(v.ctrl, WS_FLAG);
  while (flag === WS_REQ_READY) {
    Atomics.wait(v.ctrl, WS_FLAG, WS_REQ_READY);
    flag = Atomics.load(v.ctrl, WS_FLAG);
  }
  if (flag === WS_ERR) {
    const errText = dec.decode(
      v.resp.slice(0, Atomics.load(v.ctrl, WS_RESP_LEN)),
    );
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return errText.startsWith("WS_ERROR:") ? errText : "WS_ERROR:" + errText;
  }
  if (flag === WS_STREAM_END) {
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return "END";
  }
  return "CHUNK:" + Atomics.load(v.ctrl, WS_RESP_LEN);
}

// Copies current chunk into heap without advancing state.
export function wsReadStreamChunkIntoHeap(
  wsSab: SharedArrayBuffer,
  ptr: number,
  heap: Uint8Array,
): number {
  const v = wsSabViews(wsSab);
  const n = Atomics.load(v.ctrl, WS_RESP_LEN);
  heap.set(v.resp.subarray(0, n), ptr);
  return n;
}

// Advance stream; if producer already finished, short-circuit without overwriting the flag.
export function wsAdvanceStreamAndWait(
  wsSab: SharedArrayBuffer,
  dec: TextDecoder,
): string {
  const v = wsSabViews(wsSab);
  let flag = Atomics.load(v.ctrl, WS_FLAG);
  if (flag === WS_STREAM_END) {
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return "END";
  }
  if (flag === WS_IDLE) {
    return "END";
  }
  if (flag === WS_ERR) {
    const errText = dec.decode(
      v.resp.slice(0, Atomics.load(v.ctrl, WS_RESP_LEN)),
    );
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return errText.startsWith("WS_ERROR:") ? errText : "WS_ERROR:" + errText;
  }
  Atomics.store(v.ctrl, WS_FLAG, WS_CHUNK_NEXT);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
  flag = Atomics.load(v.ctrl, WS_FLAG);
  while (flag === WS_CHUNK_NEXT) {
    Atomics.wait(v.ctrl, WS_FLAG, WS_CHUNK_NEXT);
    flag = Atomics.load(v.ctrl, WS_FLAG);
  }
  if (flag === WS_ERR) {
    const errText = dec.decode(
      v.resp.slice(0, Atomics.load(v.ctrl, WS_RESP_LEN)),
    );
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return errText.startsWith("WS_ERROR:") ? errText : "WS_ERROR:" + errText;
  }
  if (flag === WS_STREAM_END) {
    Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
    Atomics.notify(v.ctrl, WS_FLAG, 1);
    return "END";
  }
  return "CHUNK:" + Atomics.load(v.ctrl, WS_RESP_LEN);
}

// Abandon in-flight stream: move channel to IDLE, wake producer (its abandon path sends FT.CANCEL).
export function wsCancelStreamAndReleaseChannel(
  wsSab: SharedArrayBuffer,
): void {
  const v = wsSabViews(wsSab);
  Atomics.store(v.ctrl, WS_FLAG, WS_IDLE);
  Atomics.notify(v.ctrl, WS_FLAG, 1);
}
