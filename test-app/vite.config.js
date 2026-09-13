import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { build as esbuild } from 'esbuild'
import path from 'path'
import fs from 'fs'
import { wasmDir } from '@n6k.io/db/node'

const REPO_DIR = wasmDir

// Plugin to serve WASM extension files from the build output
function serveExtensionRepo() {
  return {
    name: 'serve-extension-repo',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const filePath = path.join(REPO_DIR, req.url)

        if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
          const ext = path.extname(filePath)
          const mimeTypes = {
            '.wasm': 'application/wasm',
            '.json': 'application/json',
            '.js': 'text/javascript',
          }
          res.setHeader('Content-Type', mimeTypes[ext] || 'application/octet-stream')
          res.setHeader('Access-Control-Allow-Origin', '*')
          fs.createReadStream(filePath).pipe(res)
          return
        }
        next()
      })
    },
  }
}

// Bundle worker TS files into self-contained IIFE scripts for classic workers.
// Vite dev mode serves files as ES modules, but classic workers can't use
// import statements. This plugin intercepts worker file requests and runs
// esbuild to produce a single bundled script.
function bundleClassicWorkers() {
  return {
    name: 'bundle-classic-workers',
    async load(id) {
      if (!id.includes('/workers/') || !id.match(/\.(ts|js)(\?|$)/)) return null
      const filePath = id.split('?')[0]
      const result = await esbuild({
        entryPoints: [filePath],
        bundle: true,
        format: 'iife',
        write: false,
        target: 'esnext',
      })
      return result.outputFiles[0].text
    },
  }
}

export default defineConfig({
  plugins: [react(), serveExtensionRepo(), bundleClassicWorkers()],
  worker: {
    format: 'iife',
  },
  server: {
    port: 8787,
    fs: {
      allow: ['..'],
    },
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
})
