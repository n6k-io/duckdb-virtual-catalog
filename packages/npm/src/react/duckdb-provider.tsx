import { useState, useEffect, useRef, useCallback } from "react";
import { createDuckDB } from "../create-duckdb";
import type { CreateDuckDBOptions, WsStatus } from "../create-duckdb";
import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { ConnectionLike } from "../connection-shape";
import {
  configEqual,
  makeConfig,
  renderAttachSql,
  type AttachConfig,
  type AttachError,
  type AttachOptions,
} from "./attachment";
import { DuckDBContext } from "./duckdb-context";
import { logger as log } from "../logger";
import { createWasmConnect, type WasmDatabase } from "../wasm-lease";

function omitKey<V>(obj: Record<string, V>, key: string): Record<string, V> {
  if (!(key in obj)) return obj;
  const next = { ...obj };
  delete next[key];
  return next;
}

export type DatabaseSpec = string | { path: string; options?: AttachOptions };

type DuckDBProviderProps = {
  children: React.ReactNode;
  databases?: Record<string, DatabaseSpec>;
  duckdbOptions?: CreateDuckDBOptions;
};

function specToConfig(spec: DatabaseSpec): AttachConfig {
  if (typeof spec === "string") return makeConfig(spec);
  return makeConfig(spec.path, spec.options);
}

function specsToConfigs(
  specs: Record<string, DatabaseSpec> | undefined,
): Record<string, AttachConfig> {
  if (!specs) return {};
  return Object.fromEntries(
    Object.entries(specs).map(([k, v]) => [k, specToConfig(v)]),
  );
}

export function DuckDBProvider({
  children,
  databases,
  duckdbOptions,
}: DuckDBProviderProps) {
  const [conn, setConn] = useState<AsyncDuckDBConnection | null>(null);
  const [status, setStatus] = useState("initializing");
  const [error, setError] = useState<string | null>(null);
  const [connStatus, setConnStatus] = useState<Record<string, WsStatus>>({});
  const reconnectRef = useRef<(catalog: string) => void>(() => {});
  const dbRef = useRef<AsyncDuckDB | null>(null);
  const connectRef = useRef<(() => Promise<ConnectionLike>) | null>(null);
  const registerWebsocketRef = useRef<(socket: WebSocket) => string>(() => {
    throw new Error(
      "n6k: registerWebsocket() was called before the DuckDB provider was ready. " +
        "Gate it on useDuckDB().status === 'ready' (the WebSocket API is wired up " +
        "only once the worker has started and the n6k extension has loaded).",
    );
  });
  const replaceWebsocketRef = useRef<(wsId: string, socket: WebSocket) => void>(
    () => {
      throw new Error(
        "n6k: replaceWebsocket() was called before the DuckDB provider was ready. " +
          "Gate it on useDuckDB().status === 'ready'.",
      );
    },
  );
  const [desired, setDesiredState] = useState<Record<string, AttachConfig>>(
    () => specsToConfigs(databases),
  );
  const [attached, setAttached] = useState<Record<string, AttachConfig>>({});
  const [errors, setErrors] = useState<Record<string, AttachError>>({});
  const initRef = useRef(false);
  const attachedRef = useRef(attached);
  useEffect(() => {
    attachedRef.current = attached;
  }, [attached]);

  // Pre-attach SQL kept out of React state: it can carry a secret token, so it must never land in context-exposed `desired`/`attached`.
  const setupRef = useRef<Record<string, string[]>>({});

  const setDesired = useCallback(
    (catalog: string, config: AttachConfig, setup?: string[]) => {
      if (setup && setup.length > 0) {
        setupRef.current[catalog] = setup;
      } else {
        delete setupRef.current[catalog];
      }
      setDesiredState((prev) => {
        const existing = prev[catalog];
        if (existing && configEqual(existing, config)) return prev;
        return { ...prev, [catalog]: config };
      });
    },
    [],
  );

  const removeDesired = useCallback((catalog: string) => {
    delete setupRef.current[catalog];
    setDesiredState((prev) => omitKey(prev, catalog));
  }, []);

  const reconnect = useCallback(
    (catalog: string) => reconnectRef.current(catalog),
    [],
  );

  const connect = useCallback(async (): Promise<ConnectionLike> => {
    const lease = connectRef.current;
    if (!lease)
      throw new Error("duckdb not ready: connect() before database open");
    return lease();
  }, []);

  const registerWebsocket = useCallback(
    (socket: WebSocket) => registerWebsocketRef.current(socket),
    [],
  );

  const replaceWebsocket = useCallback(
    (wsId: string, socket: WebSocket) =>
      replaceWebsocketRef.current(wsId, socket),
    [],
  );

  const dropAttached = (c: string) => setAttached((prev) => omitKey(prev, c));
  const dropError = (c: string) => setErrors((prev) => omitKey(prev, c));

  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;

    createDuckDB({
      ...duckdbOptions,
      onStatus: (catalog, ws) =>
        setConnStatus((prev) => ({ ...prev, [catalog]: ws })),
    })
      .then(
        async ({
          db,
          conn,
          reconnect,
          registerWebsocket,
          replaceWebsocket,
        }) => {
          dbRef.current = db;
          connectRef.current = createWasmConnect(db as unknown as WasmDatabase);
          reconnectRef.current = reconnect;
          registerWebsocketRef.current = registerWebsocket;
          replaceWebsocketRef.current = replaceWebsocket;
          setConn(conn);
          setStatus("loading-extensions");
          log.debug("loading n6k extension...");
          await conn.query("LOAD n6k_client;");
          log.debug("n6k extension loaded");
          setStatus("ready");
          log.debug("ready");
        },
      )
      .catch((error_: unknown) => {
        log.error(error_);
        setError((error_ as Error).message);
        setStatus("error");
      });
  }, [duckdbOptions]);

  useEffect(() => {
    if (status !== "ready" || !conn) return;
    let cancelled = false;

    const detach = async (c: string) => {
      log.debug(`detaching ${c}...`);
      try {
        await conn.query(`DETACH ${c};`);
      } catch (error_) {
        log.error(`detach ${c} failed:`, error_);
      }
      dropAttached(c);
      setConnStatus((prev) => omitKey(prev, c));
    };

    const attach = async (c: string, config: AttachConfig) => {
      const sql = renderAttachSql(c, config);
      try {
        // Pre-attach setup runs first, atomically before the ATTACH it authorizes; not logged since it may embed a token.
        for (const stmt of setupRef.current[c] ?? []) {
          await conn.query(stmt);
        }
        log.debug(sql);
        await conn.query(sql);
        setAttached((prev) => ({ ...prev, [c]: config }));
        dropError(c);
        log.debug(`attached ${c}`);
      } catch (error_) {
        const message = (error_ as Error).message;
        setErrors((prev) => ({ ...prev, [c]: { config, message } }));
        log.error(`attach ${c} failed:`, error_);
      }
    };

    (async () => {
      for (const c of Object.keys(attachedRef.current)) {
        if (cancelled) return;
        if (!(c in desired)) await detach(c);
      }
      for (const [c, config] of Object.entries(desired)) {
        if (cancelled) return;
        const have = attachedRef.current[c];
        if (have && configEqual(have, config)) continue;
        if (have !== undefined) await detach(c);
        await attach(c, config);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [desired, conn, status]);

  return (
    <DuckDBContext.Provider
      value={{
        conn,
        status,
        error,
        connStatus,
        reconnect,
        registerWebsocket,
        replaceWebsocket,
        desired,
        attached,
        errors,
        setDesired,
        removeDesired,
        connect,
      }}
    >
      {children}
    </DuckDBContext.Provider>
  );
}
