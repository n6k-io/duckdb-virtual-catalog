/// <reference lib="webworker" />

import { IDLE, REQUEST, DATA_OFFSET, CATALOG_REGION_SIZE } from "../protocol";
import { applyHostDebugFlag, createLogger } from "../logger";

const log = createLogger("fetch-worker");

import {
  blockUntilRequestArrivesFromDuckDBWorker,
  loadRequestUrlLengthAndBodyLengthFromSAB,
  storeResponseAndSignalReadyToSABChannel,
  storeErrorAndSignalToSABChannel,
  storeChunkAndSignalReadyToSABChannel,
  blockUntilDuckDBWorkerRequestsChunkOrFinishes,
  signalBodyChunkRequestAndBlockUntilChunkArrives,
  loadChunkOffsetRequestedByDuckDBWorker,
  loadContentTypeLengthAlreadyStoredInSAB,
} from "../atomics";

import { findCatalogForUrl } from "../n6k-utils";

let control: Int32Array;
let urlBytes: Uint8Array;
let dataBytes: Uint8Array;
let dataCapacity: number;
let storedBuffer: Uint8Array | null = null;

let catalogLenView: Int32Array;
let catalogRegion: Uint8Array;
const catalogDecoder = new TextDecoder();

globalThis.addEventListener("message", (e: MessageEvent) => {
  if (e.data.type === "init") {
    applyHostDebugFlag(e.data.debug);
    const sab: SharedArrayBuffer = e.data.sab;
    control = new Int32Array(sab, 0, 8);
    urlBytes = new Uint8Array(sab, 32, 4096);
    dataCapacity = sab.byteLength - DATA_OFFSET - CATALOG_REGION_SIZE;
    dataBytes = new Uint8Array(sab, DATA_OFFSET, dataCapacity);
    catalogLenView = new Int32Array(
      sab,
      sab.byteLength - CATALOG_REGION_SIZE,
      2,
    );
    catalogRegion = new Uint8Array(
      sab,
      sab.byteLength - CATALOG_REGION_SIZE + 8,
      CATALOG_REGION_SIZE - 8,
    );
    pollLoop();
  }
});

async function pollLoop() {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  while (true) {
    blockUntilRequestArrivesFromDuckDBWorker(control);
    const { urlLen, bodyLen } =
      loadRequestUrlLengthAndBodyLengthFromSAB(control);
    const url = decoder.decode(urlBytes.slice(0, urlLen));
    try {
      const headers: Record<string, string> = {};
      const catalog = findCatalogForUrl(
        url,
        catalogLenView,
        catalogRegion,
        catalogDecoder,
      );
      if (catalog.token) {
        headers["Authorization"] = "Bearer " + catalog.token;
      }
      let fetchOpts: RequestInit;
      if (bodyLen < -1) {
        const totalBodyLen = -bodyLen;
        const ctLen = loadContentTypeLengthAlreadyStoredInSAB(control);
        const contentType = decoder.decode(dataBytes.slice(0, ctLen));
        headers["Content-Type"] = contentType;
        headers["Accept"] = "application/vnd.apache.arrow.stream";

        const bodyParts: Uint8Array[] = [];
        let received = 0;
        while (received < totalBodyLen) {
          const chunkLen =
            signalBodyChunkRequestAndBlockUntilChunkArrives(control);
          bodyParts.push(dataBytes.slice(0, chunkLen));
          received += chunkLen;
        }

        const fullBody = new Uint8Array(totalBodyLen);
        let off = 0;
        for (const part of bodyParts) {
          fullBody.set(part, off);
          off += part.length;
        }

        fetchOpts = {
          method: "POST",
          body: fullBody,
          headers,
        };
      } else if (bodyLen === -1) {
        fetchOpts = {
          method: "DELETE",
          headers,
        };
      } else if (bodyLen > 0) {
        const bodyBytes = urlBytes.slice(urlLen, urlLen + bodyLen);
        const body = decoder.decode(bodyBytes);
        const firstChar = body.charAt(0);
        if (firstChar === "{" || firstChar === "[") {
          headers["Content-Type"] = "application/json";
          headers["Accept"] = "application/vnd.apache.arrow.stream";
        } else {
          headers["Content-Type"] = "text/plain";
        }
        fetchOpts = {
          method: "POST",
          body,
          headers,
        };
      } else {
        headers["Accept"] = "application/vnd.apache.arrow.stream";
        fetchOpts = {
          headers,
        };
      }
      const resp = await fetch(url, fetchOpts);
      if (resp.status >= 300) {
        const errBody = await resp.text();
        const encoded = encoder.encode(errBody);
        const writeLen = Math.min(encoded.length, dataCapacity);
        dataBytes.set(encoded.subarray(0, writeLen));
        storeErrorAndSignalToSABChannel(control, writeLen, resp.status);
        continue;
      }
      storedBuffer = new Uint8Array(await resp.arrayBuffer());
      storeResponseAndSignalReadyToSABChannel(
        control,
        storedBuffer.length,
        resp.status,
      );
      while (true) {
        const flag = blockUntilDuckDBWorkerRequestsChunkOrFinishes(control);
        if (flag === IDLE || flag === REQUEST) break;
        const offset = loadChunkOffsetRequestedByDuckDBWorker(control);
        const remaining = storedBuffer.length - offset;
        const chunkLen = Math.min(remaining, dataCapacity);
        dataBytes.set(storedBuffer.subarray(offset, offset + chunkLen));
        storeChunkAndSignalReadyToSABChannel(control, chunkLen);
      }
      storedBuffer = null;
    } catch (error) {
      log.error("fetch error:", error);
      const errEncoded = encoder.encode((error as Error).message);
      const errWriteLen = Math.min(errEncoded.length, dataCapacity);
      dataBytes.set(errEncoded.subarray(0, errWriteLen));
      storeErrorAndSignalToSABChannel(control, errWriteLen, 0);
    }
  }
}
