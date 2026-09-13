import { useState, useEffect, useRef } from 'react'
import * as duckdb from '@duckdb/duckdb-wasm'
import { createN6kWorker } from '@n6k.io/db'
import fetchWorkerUrl from '@n6k.io/db/workers/fetch-worker?url'
import duckdbWorkerUrl from '@n6k.io/db/workers/duckdb-worker?url'

export function useDuckDB() {
  const [db, setDb] = useState(null)
  const [conn, setConn] = useState(null)
  const [status, setStatus] = useState('initializing')
  const [error, setError] = useState(null)
  const initRef = useRef(false)

  useEffect(() => {
    if (initRef.current) return
    initRef.current = true

    async function init() {
      try {
        setStatus('selecting bundle')
        const BUNDLES = duckdb.getJsDelivrBundles()
        const bundle = await duckdb.selectBundle(BUNDLES)

        setStatus('starting worker')
        const worker = createN6kWorker({
          mainWorkerUrl: bundle.mainWorker,
          fetchWorkerUrl,
          duckdbWorkerUrl,
        })

        const logger = new duckdb.ConsoleLogger()
        const instance = new duckdb.AsyncDuckDB(logger, worker)
        await instance.instantiate(bundle.mainModule, bundle.pthreadWorker)

        setStatus('opening database')
        await instance.open({ allowUnsignedExtensions: true })

        const connection = await instance.connect()

        setStatus('configuring extension repo')
        await connection.query(
          `SET custom_extension_repository = '${window.location.origin}';`
        )

        setDb(instance)
        setConn(connection)
        setStatus('ready')
      } catch (e) {
        setError(e.message)
        setStatus('error')
      }
    }

    init()
  }, [])

  return { db, conn, status, error }
}
