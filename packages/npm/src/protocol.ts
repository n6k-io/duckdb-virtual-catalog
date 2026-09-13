// Shared protocol constants for SharedArrayBuffer communication.

export const FLAG = 0;
export const URL_LEN = 1;
export const RESP_LEN = 2;
export const HTTP_STATUS = 3;
export const CHUNK_OFFSET = 4;
export const CHUNK_LEN = 5;
export const BODY_LEN = 7;

export const IDLE = 0;
export const REQUEST = 1;
export const RESPONSE_READY = 2;
export const ERROR = 3;
export const CHUNK_REQUEST = 4;
export const CHUNK_READY = 5;
export const BODY_CHUNK_READY = 6;
export const BODY_CHUNK_REQUEST = 7;

export const CONTROL_INTS = 8;
export const URL_OFFSET = 32;
export const URL_CAPACITY = 4096;
export const DATA_OFFSET = 4128;
export const CATALOG_REGION_SIZE = 4096;

export const SAB_SIZE = 1_048_576;
