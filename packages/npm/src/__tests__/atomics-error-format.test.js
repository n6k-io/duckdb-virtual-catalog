import { test, expect, describe } from "bun:test";
import {
  FLAG,
  RESP_LEN,
  HTTP_STATUS,
  IDLE,
  RESPONSE_READY,
  ERROR,
  SAB_SIZE,
  DATA_OFFSET,
  CONTROL_INTS,
} from "../protocol";
import { returnErrorStringIfResponseFailedAndReleaseChannel } from "../atomics";

function makeEnv() {
  const sab = new SharedArrayBuffer(SAB_SIZE);
  const ctrl = new Int32Array(sab, 0, CONTROL_INTS);
  const dataBuf = new Uint8Array(sab, DATA_OFFSET);
  return { ctrl, dataBuf };
}

describe("returnErrorStringIfResponseFailedAndReleaseChannel", () => {
  test("HTTP 4xx with body surfaces HTTP_ERROR:<status>:<body>", () => {
    const { ctrl, dataBuf } = makeEnv();
    const body =
      '{"exception_type":"ConstraintException","exception_message":"duplicate key"}';
    const bytes = new TextEncoder().encode(body);
    dataBuf.set(bytes);
    Atomics.store(ctrl, FLAG, ERROR);
    Atomics.store(ctrl, RESP_LEN, bytes.byteLength);
    Atomics.store(ctrl, HTTP_STATUS, 409);

    const got = returnErrorStringIfResponseFailedAndReleaseChannel(
      ctrl,
      dataBuf,
      new TextDecoder(),
    );
    expect(got).toBe("HTTP_ERROR:409:" + body);
    expect(Atomics.load(ctrl, FLAG)).toBe(IDLE);
  });

  test("HTTP 500 with body surfaces HTTP_ERROR:500:<body>", () => {
    const { ctrl, dataBuf } = makeEnv();
    const body = "internal server error";
    const bytes = new TextEncoder().encode(body);
    dataBuf.set(bytes);
    Atomics.store(ctrl, FLAG, ERROR);
    Atomics.store(ctrl, RESP_LEN, bytes.byteLength);
    Atomics.store(ctrl, HTTP_STATUS, 500);

    const got = returnErrorStringIfResponseFailedAndReleaseChannel(
      ctrl,
      dataBuf,
      new TextDecoder(),
    );
    expect(got).toBe("HTTP_ERROR:500:" + body);
  });

  test("transport failure (status=0) surfaces FETCH_ERROR:<body>", () => {
    const { ctrl, dataBuf } = makeEnv();
    const body = "ConnectionRefused";
    const bytes = new TextEncoder().encode(body);
    dataBuf.set(bytes);
    Atomics.store(ctrl, FLAG, ERROR);
    Atomics.store(ctrl, RESP_LEN, bytes.byteLength);
    Atomics.store(ctrl, HTTP_STATUS, 0);

    const got = returnErrorStringIfResponseFailedAndReleaseChannel(
      ctrl,
      dataBuf,
      new TextDecoder(),
    );
    expect(got).toBe("FETCH_ERROR: " + body);
  });

  test("successful response with RESPONSE_READY + 2xx status returns null", () => {
    const { ctrl, dataBuf } = makeEnv();
    Atomics.store(ctrl, FLAG, RESPONSE_READY);
    Atomics.store(ctrl, RESP_LEN, 0);
    Atomics.store(ctrl, HTTP_STATUS, 200);

    const got = returnErrorStringIfResponseFailedAndReleaseChannel(
      ctrl,
      dataBuf,
      new TextDecoder(),
    );
    expect(got).toBeNull();
  });

  test("empty body on HTTP error still surfaces HTTP_ERROR:<status>:", () => {
    const { ctrl, dataBuf } = makeEnv();
    Atomics.store(ctrl, FLAG, ERROR);
    Atomics.store(ctrl, RESP_LEN, 0);
    Atomics.store(ctrl, HTTP_STATUS, 404);

    const got = returnErrorStringIfResponseFailedAndReleaseChannel(
      ctrl,
      dataBuf,
      new TextDecoder(),
    );
    expect(got).toBe("HTTP_ERROR:404:");
  });
});
