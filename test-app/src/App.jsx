import { useState, useCallback } from 'react'
import { useDuckDB } from './useDuckDB'
import './App.css'

function App() {
  const { conn, status, error } = useDuckDB()
  const [sql, setSql] = useState("SELECT * FROM n6k('http://127.0.0.1:8000/tables/demo');")
  const [attachSql, setAttachSql] = useState("ATTACH '' AS mydb (TYPE n6k, host '127.0.0.1', port '8000');")
  const [attached, setAttached] = useState(false)
  const [results, setResults] = useState(null)
  const [queryError, setQueryError] = useState(null)
  const [extensionLoaded, setExtensionLoaded] = useState(false)

  const loadExtension = useCallback(async () => {
    if (!conn) return
    try {
      setQueryError(null)
      await conn.query('LOAD n6k_client;')
      setExtensionLoaded(true)
    } catch (e) {
      setQueryError(e.message)
    }
  }, [conn])

  const runAttach = useCallback(async () => {
    if (!conn) return
    try {
      setQueryError(null)
      await conn.query(attachSql)
      setAttached(true)
    } catch (e) {
      setQueryError(e.message)
    }
  }, [conn, attachSql])

  const runQuery = useCallback(async () => {
    if (!conn) return
    try {
      setQueryError(null)
      setResults(null)
      const result = await conn.query(sql)
      const rows = result.toArray().map(r => r.toJSON())
      setResults(rows)
    } catch (e) {
      setQueryError(e.message)
      setResults(null)
    }
  }, [conn, sql])

  return (
    <div className="app">
      <h2>DuckDB WASM - Extension Test</h2>

      <div className="status">
        Status: <span className={status === 'ready' ? 'ok' : status === 'error' ? 'err' : 'info'}>
          {status}
        </span>
        {error && <pre className="err">{error}</pre>}
      </div>

      {status === 'ready' && (
        <>
          <div className="section">
            <button onClick={loadExtension} disabled={extensionLoaded}>
              {extensionLoaded ? 'Extension Loaded' : 'Load n6k Extension'}
            </button>
          </div>

          <div className="section">
            <input
              type="text"
              value={attachSql}
              onChange={e => setAttachSql(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && runAttach()}
              className="sql-input"
            />
            <button onClick={runAttach} disabled={attached}>
              {attached ? 'Attached' : 'Attach'}
            </button>
          </div>

          <div className="section">
            <input
              type="text"
              value={sql}
              onChange={e => setSql(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && runQuery()}
              className="sql-input"
            />
            <button onClick={runQuery}>Run SQL</button>
          </div>

          {queryError && <pre className="err">{queryError}</pre>}
          {results && (
            <pre className="results">
              {JSON.stringify(results, (_, v) => typeof v === 'bigint' ? Number(v) : v, 2)}
            </pre>
          )}
        </>
      )}
    </div>
  )
}

export default App
