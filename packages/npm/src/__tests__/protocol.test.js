import { test, expect, describe } from "bun:test";
import * as P from "../protocol.js";

describe("protocol constants", () => {
  test("control array indices are distinct", () => {
    expect(P.FLAG).toBe(0);
    expect(P.URL_LEN).toBe(1);
    expect(P.RESP_LEN).toBe(2);
    expect(P.HTTP_STATUS).toBe(3);
    expect(P.CHUNK_OFFSET).toBe(4);
    expect(P.CHUNK_LEN).toBe(5);
    expect(P.BODY_LEN).toBe(7);
  });

  test("flag values are distinct", () => {
    const flags = [
      P.IDLE,
      P.REQUEST,
      P.RESPONSE_READY,
      P.ERROR,
      P.CHUNK_REQUEST,
      P.CHUNK_READY,
      P.BODY_CHUNK_READY,
      P.BODY_CHUNK_REQUEST,
    ];
    expect(new Set(flags).size).toBe(flags.length);
  });

  test("SAB layout fits within SAB_SIZE", () => {
    expect(P.DATA_OFFSET + P.CATALOG_REGION_SIZE).toBeLessThan(P.SAB_SIZE);
  });
});
