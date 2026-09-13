import type { ReactNode } from "react";
import { DuckDBContext } from "./duckdb-context";

export function NoopDuckDBProvider({ children }: { children: ReactNode }) {
  return (
    <DuckDBContext.Provider
      value={{
        conn: null,
        status: "idle",
        error: null,
        connStatus: {},
        reconnect: () => {},
        registerWebsocket: () => "",
        replaceWebsocket: () => {},
        desired: {},
        attached: {},
        errors: {},
        setDesired: () => {},
        removeDesired: () => {},
        connect: () => {
          throw new Error("connect: no database in NoopDuckDBProvider");
        },
      }}
    >
      {children}
    </DuckDBContext.Provider>
  );
}
