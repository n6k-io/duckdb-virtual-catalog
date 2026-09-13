import { Type } from "apache-arrow";
import {
  useQuery as useTanstackQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useDuckDB } from "./use-duckdb";
import {
  parseSqlTables,
  stringToTableRef,
  catalogOf,
  type TableRef,
} from "./parse-sql-tables";
import { statusOf, errorOf, configFingerprint } from "./attachment";
import { logger as log } from "../logger";
import { withLease, type ArrowLikeResult } from "../connection-shape";

type LikeSchema = ArrowLikeResult["schema"];

type QueryData = {
  columns: string[];
  rows: Record<string, unknown>[];
  schema: LikeSchema;
  tables: TableRef[];
};

type StatusInfo =
  | "noQuery"
  | "awaitingDb"
  | "awaitingAttach"
  | "executing"
  | "disconnected"
  | null;

type TabularResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  status: "idle" | "loading" | "ready" | "error";
  statusInfo: StatusInfo;
  error: string | null;
  schema: LikeSchema;
};

type CatalogRef = string | { catalog: string };

type QueryOptions = {
  tables?: Array<string | TableRef>;
  catalogs?: CatalogRef[];
};

const EMPTY: LikeSchema = { fields: [] };

function empty(
  status: TabularResult["status"],
  statusInfo: StatusInfo,
  error: string | null = null,
): TabularResult {
  return { columns: [], rows: [], status, statusInfo, error, schema: EMPTY };
}

export function useSQLQuery(
  query: string,
  options?: QueryOptions,
): TabularResult {
  const {
    conn,
    connect,
    status: dbStatus,
    desired,
    attached,
    errors,
    connStatus,
  } = useDuckDB();
  const hasExplicitTables = !!options?.tables;

  const parse = useTanstackQuery<TableRef[]>({
    queryKey: ["n6k-parse", query],
    queryFn: ({ signal }) =>
      withLease(connect, signal, (c) => parseSqlTables(c, query)),
    enabled: dbStatus === "ready" && !!conn && !!query && !hasExplicitTables,
    staleTime: Infinity,
  });

  const tables: TableRef[] = hasExplicitTables
    ? options!.tables!.map((t) =>
        typeof t === "string" ? stringToTableRef(t) : t,
      )
    : (parse.data ?? []);

  const autoCatalogs: string[] = tables
    .map((t) => catalogOf(t))
    .filter((c): c is string => c !== null);

  const explicitCatalogs: string[] = (options?.catalogs ?? []).map((r) =>
    typeof r === "string" ? r : r.catalog,
  );

  const allCatalogs = [...new Set([...autoCatalogs, ...explicitCatalogs])];
  const managed = allCatalogs.filter(
    (c) => explicitCatalogs.includes(c) || desired[c] !== undefined,
  );

  // Disconnected only if a catalog *this query touches* has a dropped socket.
  const disconnected = managed.some((c) => connStatus[c] === "disconnected");
  const attachState = { desired, attached, errors };
  const catalogStates = managed.map((c) => {
    const s = statusOf(attachState, c);
    if (s !== undefined) return s;
    // Explicit `catalogs` entry with no provider entry yet defaults to pending.
    return explicitCatalogs.includes(c) ? "pending" : "ready";
  });

  const parseReady = hasExplicitTables || parse.isSuccess;
  const parseErrored = !hasExplicitTables && parse.status === "error";
  const depError = catalogStates.includes("error");
  const depsReady = catalogStates.every((s) => s === "ready");
  const anyPending = catalogStates.includes("pending");

  const enabled =
    dbStatus === "ready" &&
    !!conn &&
    !!query &&
    parseReady &&
    depsReady &&
    !depError &&
    !disconnected;

  const result = useTanstackQuery<QueryData>({
    // Catalog socket status + attach fingerprint are in the key so a reconnect/re-attach invalidates the cached read.
    queryKey: [
      "n6k-query",
      query,
      ...managed.map((c) => connStatus[c] ?? null),
      ...managed.map((c) =>
        attached[c] ? configFingerprint(attached[c]) : null,
      ),
    ],
    queryFn: async ({ signal }) => {
      const r = await withLease(connect, signal, (c) => c.query(query));
      const schema = r.schema;
      const cols = schema.fields.map((f: { name: string }) => f.name);

      const decimalFields = schema.fields
        .filter((f) => f.type.typeId === Type.Decimal)
        .map((f) => ({
          name: f.name,
          divisor: 10 ** (f.type.scale ?? 0),
        }));

      const rows = r
        .toArray()
        .map((row: { toJSON: () => Record<string, unknown> }) => {
          const obj = row.toJSON();
          for (const { name, divisor } of decimalFields) {
            if (obj[name] != null) obj[name] = Number(obj[name]) / divisor;
          }
          return obj;
        });

      return { columns: cols, rows, schema, tables };
    },
    enabled,
  });

  if (!query) return empty("idle", "noQuery");
  if (dbStatus !== "ready" || !conn) return empty("loading", "awaitingDb");

  if (disconnected)
    return empty(
      "error",
      "disconnected",
      "Connection to the database was lost",
    );

  if (parseErrored) {
    log.error("parse error:", parse.error);
    return empty("error", null, (parse.error as Error).message);
  }

  if (depError) {
    const erroredCatalog =
      managed.find((c) => statusOf(attachState, c) === "error") ?? null;
    const msg = erroredCatalog
      ? `Attach ${erroredCatalog} failed: ${errorOf(attachState, erroredCatalog) ?? "unknown error"}`
      : "Attach failed";
    return empty("error", null, msg);
  }

  if (anyPending || !parseReady) return empty("loading", "awaitingAttach");
  if (result.status === "pending") return empty("loading", "executing");

  if (result.status === "error") {
    log.error("query error:", result.error);
    return empty("error", null, (result.error as Error).message);
  }

  return {
    columns: result.data.columns,
    rows: result.data.rows,
    status: "ready",
    statusInfo: null,
    error: null,
    schema: result.data.schema,
  };
}

export const useQuery = useSQLQuery;

export function useInvalidateQueries() {
  const queryClient = useQueryClient();
  return (pattern?: string) => {
    if (pattern) {
      queryClient.invalidateQueries({
        predicate: (query) => {
          const key = query.queryKey;
          return (
            key[0] === "n6k-query" &&
            typeof key[1] === "string" &&
            key[1].includes(pattern)
          );
        },
      });
    } else {
      queryClient.invalidateQueries({ queryKey: ["n6k-query"] });
    }
  };
}
