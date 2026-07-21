import { Navigate } from "react-router-dom";
import { coordinatorRows, DetailPanel, DiagnosticsTable, EmptyState, HealthBanner, IndexCatalog, MetricCard, PageTitle, queryRows, realtimeRows, sampleMetrics, storageRows, TrendChart, transportRows, durabilityRows } from "./components";
import { useDashboardStore } from "./store";
import { bytes, count, object, rate, valueAt } from "./utils";

function useLatest() {
  return useDashboardStore((state) => state.samples.at(-1));
}

export function OverviewPage() {
  const latest = useLatest();
  const samples = useDashboardStore((state) => state.samples);
  const { stats, rates, storage, realtime, queries } = sampleMetrics(latest);
  return <><HealthBanner sample={latest} /><section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4"><MetricCard label="Commit sequence" value={count(stats.commitSequence)} detail={`Engine ${String(storage.engine ?? (stats.durable ? "current" : "memory"))}`} tone="sky" /><MetricCard label="Commits / sec" value={rate(rates.commitsPerSecond, rates.valid)} detail={`${rate(rates.changesPerSecond, rates.valid)} changes / sec`} tone="emerald" /><MetricCard label="Queries / sec" value={rate(rates.queriesPerSecond, rates.valid)} detail={`${rate(rates.failedQueriesPerSecond, rates.valid)} failed / sec`} tone="violet" /><MetricCard label="Documents" value={count(stats.documents)} detail={`${count(stats.collections)} collections · ${count(realtime.querySubscribers)} subscribers`} tone="amber" /></section><section className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1.5fr)_minmax(320px,.8fr)]"><TrendChart samples={samples} /><DetailPanel eyebrow="Engine state" title="Current snapshot" badge={`${count(queries.activeCursors)} active cursors`} rows={[{ label: "Uptime", value: sampleMetrics(latest).uptime }, { label: "Physical bytes", value: bytes(valueAt(latest, "stats.storage.physicalBytes")) }, { label: "Retention pressure", value: String(valueAt(latest, "health.signals.commitRetentionPressure") ? "active" : "clear") }, { label: "Database mode", value: String(storage.engine ?? "—") }]} /></section></>;
}

export function StoragePage() {
  const latest = useLatest();
  const stats = sampleMetrics(latest).stats;
  return <><PageTitle eyebrow="Durability posture" title="Storage & durability" detail="Physical file health, rollback protection, retention, maintenance and backup signals. These are process-level aggregates and never expose business data." /><section className="grid gap-4 xl:grid-cols-2"><DetailPanel eyebrow="Pager" title="Storage health" badge={`${count(valueAt(latest, "stats.storage.readers"))} readers`} rows={storageRows(latest)} /><DetailPanel eyebrow="Maintenance" title="Durability" badge={sampleMetrics(latest).uptime} rows={durabilityRows(latest)} /></section><section className="mt-4 grid gap-3 sm:grid-cols-3"><MetricCard label="Physical bytes" value={count(valueAt(latest, "stats.storage.physicalBytes"))} detail="Current on-disk allocation" /><MetricCard label="Backup attempts" value={count(stats.backupAttempts)} detail={`${count(stats.backupFailures)} failed`} tone="emerald" /><MetricCard label="Reclamation conflicts" value={count(valueAt(latest, "stats.maintenance.conflicts"))} detail="Optimistic maintenance conflicts" tone="amber" /></section></>;
}

export function IndexesPage() {
  const latest = useLatest();
  const indexes = useDashboardStore((state) => state.indexes);
  const status = useDashboardStore((state) => state.indexesStatus);
  const queries = sampleMetrics(latest).queries;
  const builds = object(valueAt(latest, "stats.indexBuilds"));
  return <><PageTitle eyebrow="Deployment-managed access paths" title="Indexes & planner" detail="Published index definitions are read-only here. Build or resume indexes with the deployment CLI; this view exposes no document values, query filters, principals or tenant data." /><section className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(280px,.55fr)]"><div><div className="mb-3 flex items-center justify-between gap-3"><p className="text-xs font-semibold text-slate-600">Published catalog</p><span className="rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] font-semibold text-slate-600">{status}</span></div><IndexCatalog indexes={indexes} status={status} /></div><div className="space-y-4"><DetailPanel eyebrow="Process session" title="Planner summary" badge={`${count(queries.activeCursors)} active`} rows={queryRows(latest)} /><DetailPanel eyebrow="Resumable lifecycle" title="Index builds" badge={`${count(builds.active)} active`} rows={[{ label: "Scanning", value: count(builds.scanning) }, { label: "Catching up", value: count(builds.catchingUp) }, { label: "Ready to publish", value: count(builds.ready) }, { label: "Persistent failed", value: count(builds.persistentFailed) }, { label: "Retention pressure", value: builds.retentionPressure ? "active" : "clear" }]} /></div></section><section className="mt-4 grid gap-3 sm:grid-cols-3"><MetricCard label="Published indexes" value={count(indexes.length)} detail="Catalog entries visible to this operator" tone="violet" /><MetricCard label="Index scans" value={count(queries.indexScans)} detail="Queries planned through a secondary index" tone="emerald" /><MetricCard label="Collection scans" value={count(queries.collectionScans)} detail="Use Explain before adding an index" tone="amber" /></section></>;
}

export function RealtimePage() {
  const latest = useLatest();
  const queries = sampleMetrics(latest).queries;
  const realtime = sampleMetrics(latest).realtime;
  return <><PageTitle eyebrow="Read & publish paths" title="Realtime & queries" detail="Planner work, shared reactive views and commit-admission pressure. Use this view when a subscription falls behind or query load shifts toward collection scans." /><section className="grid gap-4 xl:grid-cols-3"><DetailPanel eyebrow="Planner" title="Query work" badge={`${count(queries.activeCursors)} active`} rows={queryRows(latest)} /><DetailPanel eyebrow="Reactive" title="Live pipeline" badge={`${count(realtime.querySubscribers)} subscribers`} rows={realtimeRows(latest)} /><DetailPanel eyebrow="Write path" title="Commit coordinator" badge={valueAt(latest, "stats.commitCoordinator.enabled") ? "enabled" : "disabled"} rows={coordinatorRows(latest)} /></section></>;
}

export function TransportPage() {
  const latest = useLatest();
  const server = latest?.server;
  return <><PageTitle eyebrow="Server boundary" title="Transport & workers" detail="Aggregate HTTP, RPC, realtime and private worker state. No principals, tenant values, method names or application payloads are exposed here." /><section className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(260px,.6fr)]"><DetailPanel eyebrow="RPC" title="Request transport" badge={`${count(valueAt(server, "rpcActive"))} active`} rows={transportRows(latest)} /><div className="space-y-3"><MetricCard label="Realtime connections" value={count(valueAt(server, "realtimeConnections"))} detail="Current public socket connections" tone="sky" /><MetricCard label="Worker connections" value={count(valueAt(server, "workerConnections"))} detail="Private control-plane workers" tone="violet" /><MetricCard label="Protocol failures" value={count(valueAt(server, "workerProtocolFailures"))} detail="Observed worker protocol errors" tone="amber" /></div></section></>;
}

export function DiagnosticsPage() {
  const events = useDashboardStore((state) => state.diagnostics);
  const enabled = useDashboardStore((state) => state.diagnosticsEnabled);
  const status = useDashboardStore((state) => state.diagnosticsStatus);
  return <><PageTitle eyebrow="Opt-in bounded ring" title="Recent diagnostics" detail="Only slow, failed or explicitly sampled operations are retained. This panel intentionally excludes request payloads, collection names, principals and tenant values." /><div className="mb-4 flex items-center gap-2"><span className={`size-2 rounded-full ${enabled ? "bg-emerald-400" : "bg-slate-500"}`} /><span className="text-xs text-slate-400">{status}</span></div><DiagnosticsTable events={events} enabled={enabled} status={status} /></>;
}

export function EmptyRoute() { return <Navigate to="/" replace />; }

export function DisconnectedPage() { return <EmptyState title="No active admin session" detail="Enter the local admin bearer token to view runtime health and telemetry. The token remains in tab memory only." />; }
