import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Dynatrace Gen3 Dashboards – JS Code Tile
 * Multi-tenant DQL executor & merger (dynamic types + tenant + metadata totals)
 */

// ---------- utils ----------
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
  let minS = Infinity, maxE = -Infinity;
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

// Fallback: infer minimal types[0] from one record if API didn’t return types
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

// ---------- network ----------
async function fetchFromDynatrace(credentialId, url, query) {
  if (!credentialId || !url || !query) throw new Error("Missing credentialId, url, or query.");

  console.log("[Fetch] URL:", url);
  const token = await credentialVaultClient
    .getCredentialsDetails({ id: credentialId })
    .then((r) => r?.token);
  if (!token) throw new Error("Empty platform token.");

  const body = { query, requestTimeoutMilliseconds: 60000, enablePreview: true };
  console.log("[Fetch] Body:", body);

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  console.log("[Fetch] Status:", res.status, res.statusText);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);

  const json = await res.json();
  console.log("[Fetch] Keys:", Object.keys(json || {}));
  return json;
}

// ---------- merger: dynamic types + tenant + totals + TZ fix ----------
function mergeMultiTenant(responses /* [{tenant, json}] */) {
  if (!responses.length) return { records: [], types: [], metadata: {}, metrics: [] };

  // Normalize & inject tenant
  const normalized = responses.map(({ tenant, json }) => {
    const n = normalize(json);
    const withTenant = n.records.map((r) => ({ ...r, tenant }));
    console.log(`[Merge] ${tenant}: recs=${withTenant.length}, types=${n.types?.length || 0}`);
    return { tenant, records: withTenant, types: n.types, metadata: n.metadata, metrics: n.metrics };
  });

  const allRecords = normalized.flatMap((n) => n.records);
  console.log("[Merge] Total merged records:", allRecords.length);

  // Dynamic types (clone first tenant’s types, or infer)
  let types0;
  const baseTypes0 = normalized[0].types?.[0];
  if (baseTypes0) {
    types0 = deepClone(baseTypes0);
    types0.mappings = types0.mappings || {};
  } else {
    types0 = inferTypesFromRecord(allRecords[0])?.[0];
  }
  if (!types0?.mappings) types0.mappings = {};
  if (!types0.mappings.tenant) types0.mappings.tenant = { type: "string" };
  const types = [types0];

  // Totals & per-tenant breakdowns
  const baseMeta  = normalized[0].metadata || {};
  const baseGrail = baseMeta.grail || {};

  const totals = { scannedRecords: 0, scannedBytes: 0, scannedDataPoints: 0, executionTimeMilliseconds: 0 };
  const perTenant = {};

  normalized.forEach((n) => {
    const g   = n.metadata?.grail || {};
    const tag = n.tenant;
    const sr  = Number(g.scannedRecords || 0);
    const sb  = Number(g.scannedBytes || 0);
    const sdp = Number(g.scannedDataPoints || 0);
    const etm = Number(n.metadata?.executionTimeMilliseconds || g.executionTimeMilliseconds || 0);

    totals.scannedRecords            += sr;
    totals.scannedBytes              += sb;
    totals.scannedDataPoints         += sdp;
    totals.executionTimeMilliseconds += etm;

    perTenant[`scannedRecords-${tag}`]            = sr;
    perTenant[`scannedBytes-${tag}`]              = sb;
    perTenant[`scannedDataPoints-${tag}`]         = sdp;
    perTenant[`executionTimeMilliseconds-${tag}`] = etm;
  });

  // Global timeframe & timezone
  const widened = globalTimeframe(allRecords, baseMeta);
  const tzCandidate = normalized.map((n) => n.metadata?.grail?.timezone).find((z) => z && z !== "Z");

  const metadata = {
    ...widened,
    grail: {
      ...(widened.grail || {}),
      timezone: tzCandidate || baseGrail.timezone || "Z",
      scannedRecords:    totals.scannedRecords,
      scannedBytes:      totals.scannedBytes,
      scannedDataPoints: totals.scannedDataPoints,
      ...perTenant, // per-tenant breakdowns here
    },
    // keep total execution time at top-level (matches Dynatrace schema)
    executionTimeMilliseconds: totals.executionTimeMilliseconds,
  };

  // Exactly one top-level metrics array (use first response)
  const metrics = normalized[0].metrics;

  // Ensure BY clause text reflects tenant (if present)
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

  return { records: allRecords, types, metadata, metrics };
}

// ---------- main ----------
export default async function () {
  // ==== configure ====
  const credentialId = "CREDENTIALS_VAULT-XXXXXXXX"; // <-- your Credentials Vault ID
  const query = "timeseries count(dt.host.cpu.user), from:-5m , by: { host.name } | limit 2"; // <-- your DQL

  // tenant name -> query endpoint
  const tenantMap = {
    prod: "https://<tenantA>.apps.dynatrace.com/platform/storage/query/v1/query:execute",
    qa:   "https://<tenantB>.apps.dynatrace.com/platform/storage/query/v1/query:execute",
  };

  const selected = ($tenant && asArray($tenant)) || Object.keys(tenantMap);
  const targets = selected.map((name) => ({ tenant: name, url: tenantMap[name] })).filter((t) => t.url);

  if (!targets.length) throw new Error("No tenant URLs resolved.");

  try {
    console.log("[Main] Tenants:", selected.join(", "));
    const fetched = await Promise.all(
      targets.map(async ({ tenant, url }) => {
        const json = await fetchFromDynatrace(credentialId, url, query);
        console.log(`[Main] ${tenant}: result keys:`, Object.keys(json?.result || json || {}));
        return { tenant, json };
      })
    );

    const result = mergeMultiTenant(fetched);
    console.log("[Main] Final merged output:", JSON.stringify(result, null, 2));
    // One and only one top-level metrics array; check quickly:
    console.log("[Main] metrics length:", (result.metrics || []).length);
    console.log("[Main] timezone:", result?.metadata?.grail?.timezone);

    return result;
  } catch (e) {
    console.error("[MultiTenantError]", e?.message || e);
    return { records: [], types: [], metadata: {}, metrics: [] };
  }
}
