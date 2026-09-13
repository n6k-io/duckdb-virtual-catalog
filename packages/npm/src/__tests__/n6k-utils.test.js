import { test, expect, describe, beforeEach } from "bun:test";
import {
  encodeCatalogData,
  findCatalogForUrl,
  resetCatalogCache,
} from "../n6k-utils.js";
import { SAB_SIZE, CATALOG_REGION_SIZE } from "../protocol.js";

function makeSABViews() {
  var sab = new SharedArrayBuffer(SAB_SIZE);
  var lenView = new Int32Array(sab, SAB_SIZE - CATALOG_REGION_SIZE, 2);
  var region = new Uint8Array(
    sab,
    SAB_SIZE - CATALOG_REGION_SIZE + 8,
    CATALOG_REGION_SIZE - 8,
  );
  return { lenView, region };
}

describe("encodeCatalogData / findCatalogForUrl", () => {
  var encoder = new TextEncoder();
  var decoder = new TextDecoder();

  beforeEach(() => {
    resetCatalogCache();
  });

  test("round-trip a single catalog entry", () => {
    var { lenView, region } = makeSABViews();
    var catalogs = new Map([["http://host/app1", { t: "tok1" }]]);
    encodeCatalogData(catalogs, lenView, region, encoder);

    var result = findCatalogForUrl(
      "http://host/app1/tables",
      lenView,
      region,
      decoder,
    );
    expect(result.token).toBe("tok1");
  });

  test("multiple catalogs with longest-prefix match", () => {
    var { lenView, region } = makeSABViews();
    var catalogs = new Map([
      ["http://host", { t: "tok-root" }],
      ["http://host/app1", { t: "tok-app1" }],
      ["http://host/app2", { t: "tok-app2" }],
    ]);
    encodeCatalogData(catalogs, lenView, region, encoder);

    expect(
      findCatalogForUrl("http://host/app1/tables", lenView, region, decoder)
        .token,
    ).toBe("tok-app1");
    expect(
      findCatalogForUrl("http://host/app2/schemas", lenView, region, decoder)
        .token,
    ).toBe("tok-app2");
    expect(
      findCatalogForUrl("http://host/other", lenView, region, decoder).token,
    ).toBe("tok-root");
  });

  test("empty map returns null token", () => {
    var { lenView, region } = makeSABViews();
    var result = findCatalogForUrl(
      "http://host/app1/tables",
      lenView,
      region,
      decoder,
    );
    expect(result.token).toBeNull();
  });

  test("no matching prefix returns null token", () => {
    var { lenView, region } = makeSABViews();
    var catalogs = new Map([["http://other-host/app1", { t: "tok1" }]]);
    encodeCatalogData(catalogs, lenView, region, encoder);

    var result = findCatalogForUrl(
      "http://host/app1/tables",
      lenView,
      region,
      decoder,
    );
    expect(result.token).toBeNull();
  });

  test("empty token returns null", () => {
    var { lenView, region } = makeSABViews();
    var catalogs = new Map([["http://host/app1", { t: "" }]]);
    encodeCatalogData(catalogs, lenView, region, encoder);

    var result = findCatalogForUrl(
      "http://host/app1/tables",
      lenView,
      region,
      decoder,
    );
    expect(result.token).toBeNull();
  });

  test("cache invalidation on update", () => {
    var { lenView, region } = makeSABViews();
    var catalogs = new Map([["http://host/app1", { t: "tok1" }]]);
    encodeCatalogData(catalogs, lenView, region, encoder);

    var r1 = findCatalogForUrl(
      "http://host/app1/tables",
      lenView,
      region,
      decoder,
    );
    expect(r1.token).toBe("tok1");

    catalogs.set("http://host/app1", { t: "tok2" });
    encodeCatalogData(catalogs, lenView, region, encoder);

    var r2 = findCatalogForUrl(
      "http://host/app1/tables",
      lenView,
      region,
      decoder,
    );
    expect(r2.token).toBe("tok2");
  });
});
