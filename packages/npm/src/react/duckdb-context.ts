import { createContext } from "react";
import type { ConnectionLike } from "../connection-shape";
import type { WsStatus } from "../types";
import type { AttachConfig, AttachError } from "./attachment";

export type DuckDBContextValue = {
  conn: ConnectionLike | null;
  status: string;
  error: string | null;
  connStatus: Record<string, WsStatus>;
  reconnect: (catalog: string) => void;
  registerWebsocket: (socket: WebSocket) => string;
  replaceWebsocket: (wsId: string, socket: WebSocket) => void;
  desired: Record<string, AttachConfig>;
  attached: Record<string, AttachConfig>;
  errors: Record<string, AttachError>;
  // `setup` runs before this catalog's ATTACH in the same reconcile step; kept provider-local so token-bearing SQL never leaks and is excluded from the fingerprint.
  setDesired: (catalog: string, config: AttachConfig, setup?: string[]) => void;
  removeDesired: (catalog: string) => void;
  // Lease a connection per read: a DuckDB connection holds one query slot, so shared reads collide (loser returns zero rows). Throws until ready.
  connect: () => Promise<ConnectionLike>;
};

export const DuckDBContext = createContext<DuckDBContextValue | null>(null);
