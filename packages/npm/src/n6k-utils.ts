export type CatalogEntry = { t: string };

// Serialize catalogs to the SAB region; lenView[0]=version (bumped each write), lenView[1]=byte-length.
export function encodeCatalogData(
  catalogs: Map<string, CatalogEntry>,
  lenView: Int32Array,
  region: Uint8Array,
  encoder: TextEncoder,
): void {
  const obj: Record<string, CatalogEntry> = {};
  for (const [k, v] of catalogs) {
    obj[k] = v;
  }
  const encoded = encoder.encode(JSON.stringify(obj));
  const len = Math.min(encoded.length, region.length);
  region.set(encoded.subarray(0, len));
  Atomics.store(lenView, 1, len);
  Atomics.add(lenView, 0, 1);
}

let _cachedVersion = -1;
let _cachedData: Record<string, CatalogEntry> = {};

// Look up a URL's bearer token by longest-prefix match against the SAB catalog data; caches parsed JSON per version.
export function findCatalogForUrl(
  url: string,
  lenView: Int32Array,
  region: Uint8Array,
  decoder: TextDecoder,
): { token: string | null } {
  const version = Atomics.load(lenView, 0);
  const len = Atomics.load(lenView, 1);
  if (len <= 0) return { token: null };
  if (version !== _cachedVersion) {
    _cachedData = JSON.parse(decoder.decode(region.slice(0, len)));
    _cachedVersion = version;
  }
  let bestKey = "";
  for (const key in _cachedData) {
    if (url.startsWith(key) && key.length > bestKey.length) {
      bestKey = key;
    }
  }
  if (!bestKey) return { token: null };
  const entry = _cachedData[bestKey];
  return { token: entry?.t || null };
}

export function resetCatalogCache(): void {
  _cachedVersion = -1;
  _cachedData = {};
}
