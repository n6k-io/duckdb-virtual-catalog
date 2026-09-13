import type { Schema } from "apache-arrow";

export type WsStatus = "connected" | "disconnected" | "reconnecting" | "error";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnySchema = Schema<any>;
