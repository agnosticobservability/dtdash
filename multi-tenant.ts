import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Dynatrace Gen3 Dashboards – JS Code Tile
 * Multi-tenant DQL executor & merger
 *
 * - Executes the same DQL against multiple tenants
 * - Merges results preserving the original DQL schema
 * - Injects an extra dimension/column: `tenant`
 * - Builds `types` dynamically from the API response (no hard-coding)
 * - Widens metadata.grail.analysisTimeframe to the global min/max
 * - Keeps a single top-level `metrics` array
 */

// ----------------- utilities -----------------
const asArray = (v) => (Array.isArray(v) ? v : v ? [v] : []);
const deepClone = (obj) => JSON.parse(JSON.stringify(obj));
const parseTs = (s) => (typeof s === "string" ? Date.parse(s) : Number(s));

const normalize = (json) => {
  const r = json?.result ?? json ?? {};
  return {
    records: asArray(r.records),
    types: asArray(r.types),
    metadata: r.metadata ?? {},
    metrics: asArray(r.metrics),
  };
};

const globalTimeframe = (records, baseMeta) => {
  let minS = Infinity,
    maxE = -Infinity;
  for (const rec of records) {
    const s = parseTs(rec?.timeframe?.start);
    const e = parseTs(rec?.timeframe?.end);
    if (Number.isFinite(s)) minS = Math.min(minS, s);
    if (Number.isFinite(e)) maxE = Math.max(maxE, e);
  }
  const iso = (ms) => new Date(ms).toISOString();
  const grail = baseMeta?.grail ?? {};
  return {
    ...baseMeta,
    grail: {
      ...grail,
      analysisTimeframe:
        Number.isFinite(minS) && Number.isFinite(maxE)
          ? { start: iso(minS), end: iso(maxE) }
          : grail.analysisTimeframe,
    },
  };
};

// Fallback: infer minimal types[0] from one record (used only if API didn't return types)
function inferTypesFromRecord(sample) {
  const mappings = {};
  for (const [k, v] of Object.entries(sample ?? {})) {
    if (k === "timeframe" && v && typeof v === "object" && "start" in v && "end" in v) {
      mappings[k] = { type: "timeframe" };
    } else if (k === "interval" && (typeof v === "number" || /^\d+$/.test(String(v)))) {
      mappings[k] = { type: "duration" };
    } else if (Array.isArray(v)) {
      const eltType = v.find((x) => x != null);
      mappings[k] = {
        type: "array",
        types: [
          {
            indexRange: [0, Math.max(0, v.length - 1)],
            mappings: { element: { type: typeof eltType === "number" ? "double" : "string" } },
          },
        ],
      };
    } else {
      mappings[k] = { type: "string" };
    }
  }
  return [{ indexRange: [0, 1], mappings }];
}

// ----------------- network -----------------
async function fetchFromDynatrace(credentialId, url, query) {
  if (!credentialId || !url || !query) throw new Error("Missing credentialId, url, or query.");

  const token = await credentialVaultClient
    .getCredentialsDetails({ id: credentialId })
    .then((r) => r?.token);
  if (!token) throw new Error("Empty platform token.");

  const body = { query, requestTimeoutMilliseconds: 60000, enablePreview: true };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return res.json();
}

// ----------------- merger: dynamic types + tenant (with totals) -----------------
function mergeMultiTenant(responses /* [{tenant, json}] */) {
  if (!responses.length) return { records: [], types: [], metadata: {}, metrics: [] };

  // Normalize and inject tenant column per record
  const normalized = responses.map(({ tenant, json }) => {
    const n = normalize(json);
    const withTenant = n.records.map((r) => ({ ...r, tenant }));
    return { tenant, records: withTenant, types: n.types, metadata: n.metadata, metrics: n.metrics };
  });

  const allRecords = normalized.flatMap((n) => n.records);

  // Build types dynamically from the first response (or infer)
  let types0;
  const baseTypes0 = normalized[0].types?.[0];
  if (baseTypes0) {
    types0 = deepClone(baseTypes0);
    types0.mappings = types0.mappings || {};
  } else {
    const inferred = inferTypesFromRecord(allRecords[0]) || [];
    types0 = inferred[0];
  }
  if (!types0.mappings.tenant) types0.mappings.tenant = { type: "string" };
  const types = [types0];

  // ---- Totals & per-tenant breakdowns ----
  const baseMeta = normalized[0].metadata || {};
  const baseGrail = baseMeta.grail || {};

  const totals = { scannedRecords: 0, scannedBytes: 0, scannedDataPoints: 0, executionTimeMilliseconds: 0 };
  const perTenant = {};

  normalized.forEach((n) => {
    const g = n.metadata?.grail || {};
    const t = n.tenant;
    const sr  = Number(g.scannedRecords || 0);
    const sb  = Number(g.scannedBytes || 0);
    const sdp = Number(g.scannedDataPoints || 0);
    const etm = Number(n.metadata?.executionTimeMilliseconds || g.executionTimeMilliseconds || 0);

    totals.scannedRecords            += sr;
    totals.scannedBytes              += sb;
    totals.scannedDataPoints         += sdp;
    totals.executionTimeMilliseconds += etm;

    perTenant[`scannedRecords-${t}`]            = sr;
    perTenant[`scannedBytes-${t}`]              = sb;
    perTenant[`scannedDataPoints-${t}`]         = sdp;
    perTenant[`executionTimeMilliseconds-${t}`] = etm;
  });

  // Widen timeframe (min start / max end across all records)
  const widened = globalTimeframe(allRecords, baseMeta);

  // Prefer the first non-"Z" timezone if any tenant provides one
  const tzCandidate = normalized
    .map((n) => n.metadata?.grail?.timezone)
    .find((z) => z && z !== "Z");

  const metadata = {
    ...widened,
    grail: {
      ...(widened.grail || {}),
      timezone: tzCandidate || baseGrail.timezone || "Z",
      scannedRecords:     totals.scannedRecords,
      scannedBytes:       totals.scannedBytes,
      scannedDataPoints:  totals.scannedDataPoints,
      // per-tenant breakdowns merged below
    },
    // Keep total execution time at the top level (mirrors API field)
    executionTimeMilliseconds: totals.executionTimeMilliseconds,
  };
  metadata.grail = { ...(metadata.grail || {}), ...perTenant };

  // Keep metrics from first response (identical for same query)
  const metrics = normalized[0].metrics;

  // Cosmetic: ensure tenant appears in BY clause text if those strings exist
  if (metadata?.grail) {
    const addTenantToBy = (txt) =>
      typeof txt === "string"
        ? txt.replace(/by:\s*\{\s*([^}]*)\s*\}/i, (_m, g1) => {
            const parts = g1.split(",").map((s) => s.trim()).filter(Boolean);
            if (!parts.includes("tenant")) parts.push("tenant");
            return `by:{${parts.join(", ")}}`;
          })
        : txt;
    metadata.grail.canonicalQuery = addTenantToBy(metadata.grail.canonicalQuery);
    metadata.grail.query          = addTenantToBy(metadata.grail.query);
  }

  // Return exactly the expected top-level keys; no duplicates
  return { records: allRecords, types, metadata, metrics };
}


// ----------------- main entry -----------------
export default async function () {
  // ===== Configure your environment =====
  const credentialId = "CREDENTIALS_VAULT-XXXXXXXX"; // TODO: replace with your Credentials Vault ID
  const query = "timeseries count(dt.host.cpu.user), from:-5m , by: { host.name } | limit 2"; // TODO: set your DQL

  // Map of tenant name -> platform query endpoint
  const tenantMap = {
    prod: "https://<tenantA>.apps.dynatrace.com/platform/storage/query/v1/query:execute",
    qa: "https://<tenantB>.apps.dynatrace.com/platform/storage/query/v1/query:execute",
    // dev: "https://<tenantC>.apps.dynatrace.com/platform/storage/query/v1/query:execute",
  };

  // Choose tenants
  const selected = ($tenant && asArray($tenant)) || Object.keys(tenantMap);
  const targets = selected
    .map((name) => ({ tenant: name, url: tenantMap[name] }))
    .filter((t) => t.url);

  if (!targets.length) throw new Error("No tenant URLs resolved.");

  try {
    const fetched = await Promise.all(
      targets.map(async ({ tenant, url }) => ({ tenant, json: await fetchFromDynatrace(credentialId, url, query) }))
    );

    // Return merged DQL-shaped object with extra `tenant` column
    return mergeMultiTenant(fetched);
  } catch (e) {
    console.error("[MultiTenantError]", e?.message || e);
    return { records: [], types: [], metadata: {}, metrics: [] };
  }
}
