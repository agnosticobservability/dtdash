import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

// ========= helpers =========
function asArray(v) { return Array.isArray(v) ? v : (v ? [v] : []); }

function normalizeResult(json) {
  // API may return { result: {...} } or plain {...}
  const res = json?.result ?? json;
  return {
    records: asArray(res?.records),
    types: asArray(res?.types),
    metadata: res?.metadata ?? json?.metadata ?? {},
    metrics: asArray(res?.metrics ?? json?.metrics),
  };
}

function parseTs(ts) {
  // ts is ISO with or without TZ (e.g., "...-04:00" or "...Z")
  // Date.parse returns ms; keep it numeric for min/max math
  return typeof ts === "string" ? Date.parse(ts) : Number(ts);
}

function timeframeOfRecord(rec) {
  const start = parseTs(rec?.timeframe?.start);
  const end = parseTs(rec?.timeframe?.end);
  return { start, end };
}

function widenAnalysisTimeframe(records, baseMeta) {
  let minStart = Number.POSITIVE_INFINITY;
  let maxEnd = Number.NEGATIVE_INFINITY;

  for (const r of records) {
    const { start, end } = timeframeOfRecord(r);
    if (Number.isFinite(start)) minStart = Math.min(minStart, start);
    if (Number.isFinite(end))   maxEnd   = Math.max(maxEnd, end);
  }

  const tf = (isFinite(minStart) && isFinite(maxEnd))
    ? {
        start: new Date(minStart).toISOString(),
        end: new Date(maxEnd).toISOString(),
      }
    : (baseMeta?.grail?.analysisTimeframe ?? baseMeta?.analysisTimeframe);

  return {
    ...baseMeta,
    grail: {
      ...(baseMeta?.grail ?? {}),
      analysisTimeframe: tf,
    },
    analysisTimeframe: undefined, // keep only under grail to mirror your sample
  };
}

async function fetchFromDynatrace(credentialId, url, query) {
  if (!credentialId || !url || !query) {
    throw new Error("[ValidationError] Missing required parameters: credentialId, url, or query.");
  }

  let token;
  try {
    token = await credentialVaultClient
      .getCredentialsDetails({ id: credentialId })
      .then((res) => res?.token);
  } catch (e) {
    throw new Error("Unable to fetch platform token.");
  }
  if (!token) throw new Error("[CredentialVaultError] Token is undefined or empty.");

  const body = {
    query,
    requestTimeoutMilliseconds: 60000,
    enablePreview: true,
  };

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`[HTTPError] API call failed with status ${response.status}: ${response.statusText}`);
  }
  return response.json();
}

// ========= shape merger (adds tenant) =========
function mergeMultiTenant(responses /* [{tenant, json}] */) {
  if (!responses.length) {
    return { records: [], types: [], metadata: {}, metrics: [] };
  }

  // Normalize all
  const normalized = responses.map(({ tenant, json }) => {
    const n = normalizeResult(json);
    // Inject tenant into each record
    const recordsWithTenant = (n.records || []).map((r) => ({ ...r, tenant }));
    return { tenant, ...n, records: recordsWithTenant };
  });

  // Merge records
  const allRecords = normalized.flatMap((n) => n.records);

  // Base types/metadata/metrics from first tenant
  const base = normalized[0];

  // Clone types[0] and add tenant mapping
  const baseTypes = asArray(base.types);
  const t0 = baseTypes[0] ? JSON.parse(JSON.stringify(baseTypes[0])) : { indexRange: [0, 0], mappings: {} };
  t0.mappings = t0.mappings || {};
  t0.mappings.tenant = { type: "string" };

  // Optionally update indexRange high bound to reflect number of "columns" present; keep your original as-is.
  const mergedTypes = [t0];

  // Merge metadata: keep first, widen timeframe; sum scannedDataPoints if present
  const metaSumDataPoints = normalized.reduce(
    (acc, n) => acc + Number(n.metadata?.grail?.scannedDataPoints || 0),
    0
  );
  const metadata = widenAnalysisTimeframe(allRecords, {
    ...base.metadata,
    grail: {
      ...(base.metadata?.grail ?? {}),
      scannedDataPoints: metaSumDataPoints || base.metadata?.grail?.scannedDataPoints,
    },
  });

  // Keep metrics from first (they’re identical across tenants for same query)
  const metrics = base.metrics;

  return {
    records: allRecords,
    types: mergedTypes,
    metadata,
    metrics,
  };
}

// ========= main =========
export default async function () {
  // === YOUR SETTINGS ===
  const credentialId = "CREDENTIALS_VAULT-XXXXXXXXXXXX?"; // <-- keep your real id
  const query =
    "timeseries count(dt.host.cpu.user), from:-5m , by: { host.name } | limit 2";

  // Map of tenant name -> query endpoint
  const tenantMap = {
    prod: "https://xxx.apps.dynatrace.com/platform/storage/query/v1/query:execute",
    dev: "https://yyy.apps.dynatrace.com/platform/storage/query/v1/query:execute",
    // ...
  };

  // Selected tenants (e.g., from dashboard variable)
  const selectedTenantsDashboard = $tenant || Object.keys(tenantMap);
  const urls = asArray(selectedTenantsDashboard)
    .map((name) => ({ tenant: name, url: tenantMap[name] }))
    .filter((t) => !!t.url);

  if (!urls.length) throw new Error("No tenant URLs resolved. Check tenant map / selection.");

  try {
    const responses = await Promise.all(
      urls.map(async ({ tenant, url }) => {
        const json = await fetchFromDynatrace(credentialId, url, query);
        return { tenant, json };
      })
    );

    // Build merged, DQL-shaped structure with extra "tenant" column
    const merged = mergeMultiTenant(responses);

    // Optional: tweak metadata.grail.canonicalQuery to document tenant dimension
    // (purely cosmetic; remove if you want original verbatim)
    const q = (merged?.metadata?.grail?.query || query);
    merged.metadata.grail = {
      ...(merged.metadata.grail || {}),
      canonicalQuery: (merged.metadata.grail?.canonicalQuery || q).replace(
        /by:\s*\{([^}]*)\}/,
        (m, g1) => `by:{${g1.trim().length ? g1.trim() + ", tenant" : "tenant"}}`
      ),
      query: q.replace(
        /by:\s*\{([^}]*)\}/,
        (m, g1) => `by: { ${g1.trim().length ? g1.trim() + ", tenant" : "tenant"} }`
      ),
    };

    return merged;
  } catch (error) {
    console.error(`[MainFunctionError] ${error.message}`);
    return { records: [], types: [], metadata: {}, metrics: [] };
  }
}
