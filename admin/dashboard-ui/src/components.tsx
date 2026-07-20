import type { ReactNode } from "react";
import { NavLink, Outlet } from "react-router-dom";
import type { AdminSample, DiagnosticEvent } from "./types";
import { bytes, count, duration, number, object, percent, rate, ratio, time, uptime, valueAt } from "./utils";
import { useDashboardStore } from "./store";

type IconName = "overview" | "storage" | "realtime" | "transport" | "diagnostics" | "logout";

function Icon({ name }: { name: IconName }): ReactNode {
  const paths: Record<IconName, ReactNode> = {
    overview: <><path d="M4 13h6V4H4v9Zm0 7h6v-4H4v4Zm10 0h6v-9h-6v9Zm0-16v4h6V4h-6Z" /></>,
   storage: <><ellipse cx="12" cy="5" rx="7" ry="3" /><path d="M5 5v7c0 1.7 3.1 3 7 3s7-1.3 7-3V5M5 12v7c0 1.7 3.1 3 7 3s7-1.3 7-3v-7" /></>,
    realtime: <><path d="M4 18a11 11 0 0 1 16 0M7 15a7 7 0 0 1 10 0M10 12a3 3 0 0 1 4 0" /><circle cx="12" cy="19" r="1" /></>,
    transport: <><path d="M7 7h10v10H7zM4 12H2m20 0h-2M12 4V2m0 20v-2" /><path d="M10 10h4v4h-4z" /></>,
    diagnostics: <><path d="M9 3h6l1 2h3v16H5V5h3l1-2Z" /><path d="M9 12h6M9 16h4" /></>,
    logout: <><path d="M10 4H5v16h5M14 8l4 4-4 4M18 12H9" /></>,
  };
  return <svg viewBox="0 0 24 24" aria-hidden="true" className="size-4 fill-none stroke-current stroke-[1.8]">{paths[name]}</svg>;
}

const navigation: { to: string; label: string; icon: IconName }[] = [
  { to: "/", label: "Overview", icon: "overview" },
  { to: "/storage", label: "Storage & durability", icon: "storage" },
  { to: "/realtime", label: "Realtime & queries", icon: "realtime" },
  { to: "/transport", label: "Transport & workers", icon: "transport" },
  { to: "/diagnostics", label: "Diagnostics", icon: "diagnostics" },
];

export function AppShell({ children }: { children?: ReactNode }) {
  const connection = useDashboardStore((state) => state.connection);
  const label = useDashboardStore((state) => state.connectionLabel);
  const disconnect = useDashboardStore((state) => state.disconnect);
  const latest = useDashboardStore((state) => state.samples.at(-1));
  const health = latest?.health?.overall ?? "unavailable";
  return <div className="min-h-screen bg-slate-50 text-slate-950 selection:bg-indigo-200">
    <div className="grid min-h-screen w-full lg:grid-cols-[264px_minmax(0,1fr)]">
      <aside className="border-b border-slate-200 bg-white px-4 py-4 lg:sticky lg:top-0 lg:h-screen lg:border-b-0 lg:border-r lg:px-4 lg:py-5">
        <div className="flex items-center gap-3 px-2">
          <div className="grid size-9 place-items-center rounded-xl bg-indigo-600 font-black text-white shadow-sm shadow-indigo-200">M</div>
          <div><p className="text-sm font-semibold tracking-tight text-slate-950">Meldbase</p><p className="text-[10px] font-semibold uppercase tracking-[.14em] text-slate-500">Operator console</p></div>
        </div>
        <nav className="mt-7 flex gap-1 overflow-x-auto pb-1 lg:flex-col lg:overflow-visible" aria-label="Observability sections">
          {navigation.map((item) => <NavLink key={item.to} to={item.to} end={item.to === "/"} className={({ isActive }) =>
            `flex shrink-0 items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition ${isActive ? "bg-indigo-50 text-indigo-700" : "text-slate-600 hover:bg-slate-100 hover:text-slate-950"}`
          }><Icon name={item.icon} />{item.label}</NavLink>)}
        </nav>
        <div className="mt-7 hidden rounded-xl border border-slate-200 bg-slate-50 p-3 lg:block">
          <p className="text-[10px] font-semibold uppercase tracking-[.14em] text-slate-500">Session health</p>
          <div className="mt-2 flex items-center justify-between"><StatusDot state={connection} /><span className={`rounded-md px-2 py-1 text-xs font-semibold capitalize ${healthTone(health)}`}>{health}</span></div>
          <p className="mt-2 text-xs text-slate-500">{label}</p>
        </div>
        <button onClick={disconnect} className="mt-4 hidden w-full items-center justify-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-600 transition hover:border-rose-200 hover:bg-rose-50 hover:text-rose-700 lg:flex"><Icon name="logout" /> Disconnect</button>
      </aside>
      <main className="min-w-0 px-4 py-5 sm:px-6 lg:px-8 lg:py-7 xl:px-10">
        <header className="mb-6 flex flex-wrap items-end justify-between gap-4 border-b border-slate-200 pb-5">
          <div><p className="text-[10px] font-semibold uppercase tracking-[.16em] text-indigo-600">Live process telemetry</p><h1 className="mt-1 text-2xl font-semibold tracking-tight text-slate-950 sm:text-3xl">Database operations</h1></div>
          <div className="flex items-center gap-3"><div className="text-right text-xs text-slate-500"><p>Last sample</p><p className="mt-0.5 font-semibold text-slate-700">{time(valueAt(latest, "stats.capturedAt"))}</p></div><StatusDot state={connection} /></div>
        </header>
        {children ?? <Outlet />}
      </main>
    </div>
  </div>;
}

export function StatusDot({ state }: { state: string }) {
  const tone = state === "live" ? "bg-emerald-500" : state === "retrying" || state === "connecting" ? "bg-amber-400" : state === "error" ? "bg-rose-500" : "bg-slate-400";
  return <span className="inline-flex items-center gap-2 text-xs font-medium text-slate-600"><span className={`size-2 rounded-full ${tone}`} />{state === "live" ? "Connected" : state}</span>;
}

export function PageTitle({ eyebrow, title, detail }: { eyebrow: string; title: string; detail: string }) {
  return <div className="mb-6"><p className="text-[10px] font-semibold uppercase tracking-[.16em] text-indigo-600">{eyebrow}</p><h2 className="mt-1 text-xl font-semibold tracking-tight text-slate-950">{title}</h2><p className="mt-1 max-w-4xl text-sm leading-6 text-slate-600">{detail}</p></div>;
}

export function HealthBanner({ sample }: { sample?: AdminSample }) {
  const health = sample?.health;
  const level = health?.overall ?? "unavailable";
  const labels: Record<string, string> = {
   databaseClosed: "Database closed", writesDisabled: "Writes fail-stopped", reactiveQueuePressure: "Reactive queue pressure",
    reactiveQueueOverflow: "Reactive queue overflow", slowConsumer: "Slow consumer disconnected", persistentFreeSpaceDiscarded: "Persistent free map discarded",
   commitRetentionPressure: "Commit history retention pressure", commitCoordinatorPressure: "Commit coordinator admission pressure",
   commitCoordinatorRejected: "Commit coordinator rejected a write", primaryWriteFenceRejected: "Primary write fence rejected a write",
    indexBuildFailed: "Index build requires attention", indexBuildRetentionPressure: "Index build pins retained history",
   storageQuotaExhausted: "Storage quota exhausted", storageLimitRejected: "Storage limit rejected a write",
    durabilityFailure: "Durability operation failed", rollbackAnchorDegraded: "Rollback anchor degraded",
    telemetryDeliveryDropped: "Telemetry delivery dropped", transportBusy: "Transport concurrency busy", rpcOutcomeUnknown: "RPC outcome unknown",
    workerProtocolFailure: "Worker protocol failure",
  };
  const active = Object.entries(health?.signals ?? {}).filter(([, enabled]) => enabled).map(([name]) => labels[name] ?? name);
  const components = ["database", "durability", "storage", "realtime", "telemetry", "transport"] as const;
  return <section className={`mb-5 rounded-xl border p-5 ${healthPanelTone(level)} `}><div className="flex flex-col gap-5 lg:flex-row lg:items-center lg:justify-between"><div><div className="flex items-center gap-2"><span className={`size-2 rounded-full ${healthDot(level)}`} /><p className="text-xs font-semibold uppercase tracking-[.16em]">Overall engine health</p></div><p className="mt-2 text-2xl font-semibold capitalize text-slate-950">{level}</p><p className="mt-1 text-sm text-slate-600">{active.length ? active.join(" · ") : "No active engine health signals."}</p></div><div className="grid grid-cols-2 gap-x-7 gap-y-3 text-xs sm:grid-cols-3">{components.map((name) => <div key={name}><p className="uppercase tracking-wide text-slate-500">{name}</p><p className={`mt-1 font-semibold capitalize ${healthText(health?.[name] ?? "unavailable")}`}>{health?.[name] ?? "unavailable"}</p></div>)}</div></div></section>;
}

export function MetricCard({ label, value, detail, tone = "sky" }: { label: string; value: string; detail: string; tone?: "sky" | "emerald" | "violet" | "amber" }) {
  const tones = { sky: "bg-sky-500", emerald: "bg-emerald-500", violet: "bg-violet-500", amber: "bg-amber-500" };
  return <article className="relative overflow-hidden rounded-xl border border-slate-200 bg-white p-5 shadow-sm"><div className={`absolute inset-x-0 top-0 h-1 ${tones[tone]}`} /><p className="text-xs font-medium text-slate-500">{label}</p><p className="mt-5 text-3xl font-semibold tracking-tight text-slate-950">{value}</p><p className="mt-1 text-xs text-slate-500">{detail}</p></article>;
}

export function DetailPanel({ eyebrow, title, badge, rows }: { eyebrow: string; title: string; badge?: string; rows: { label: string; value: string }[] }) {
  return <article className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm"><div className="flex items-start justify-between gap-4"><div><p className="text-[10px] font-semibold uppercase tracking-[.16em] text-indigo-600">{eyebrow}</p><h3 className="mt-1 text-base font-semibold text-slate-950">{title}</h3></div>{badge && <span className="rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] font-semibold text-slate-600">{badge}</span>}</div><dl className="mt-5 divide-y divide-slate-100">{rows.map((row) => <div key={row.label} className="flex items-center justify-between gap-4 py-2.5"><dt className="text-xs text-slate-500">{row.label}</dt><dd className="text-right text-xs font-semibold text-slate-700">{row.value}</dd></div>)}</dl></article>;
}

export function TrendChart({ samples }: { samples: AdminSample[] }) {
  const valid = samples.filter((sample) => Boolean(valueAt(sample, "rates.valid")));
  const max = Math.max(1, ...valid.flatMap((sample) => [number(valueAt(sample, "rates.commitsPerSecond")), number(valueAt(sample, "rates.queriesPerSecond"))]));
  const points = (key: string) => valid.map((sample, index) => {
    const x = valid.length < 2 ? 900 : index / (valid.length - 1) * 900;
    const y = 188 - number(valueAt(sample, key)) / max * 170;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm"><div className="flex flex-wrap items-center justify-between gap-3"><div><p className="text-[10px] font-semibold uppercase tracking-[.16em] text-indigo-600">Last samples</p><h3 className="mt-1 font-semibold text-slate-950">Workload pulse</h3></div><div className="flex gap-4 text-xs text-slate-500"><span><i className="mr-1.5 inline-block size-2 rounded-full bg-emerald-500" />Commits/s</span><span><i className="mr-1.5 inline-block size-2 rounded-full bg-sky-500" />Queries/s</span></div></div><svg viewBox="0 0 900 200" role="img" aria-label="Commit and query rates over time" className="mt-5 h-44 w-full overflow-visible sm:h-52">{[40, 80, 120, 160].map((y) => <line key={y} x1="0" x2="900" y1={y} y2={y} className="stroke-slate-200" strokeDasharray="4 8" />)}<polyline points={points("rates.commitsPerSecond")} className="fill-none stroke-emerald-500" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" /><polyline points={points("rates.queriesPerSecond")} className="fill-none stroke-sky-500" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" /></svg></section>;
}

export function DiagnosticsTable({ events, enabled, status }: { events: DiagnosticEvent[]; enabled: boolean; status: string }) {
  if (!enabled) return <EmptyState title="Diagnostics are disabled" detail="Enable bounded diagnostics on the server to retain slow, failed or sampled operations." />;
  if (!events.length) return <EmptyState title="No retained diagnostic events" detail={status === "temporarily unavailable" ? "The diagnostic endpoint is temporarily unavailable." : "No slow, failed or sampled operations have been retained."} />;
  return <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-sm"><table className="w-full min-w-[720px] border-collapse text-left text-xs"><thead className="bg-slate-50 text-[10px] uppercase tracking-[.14em] text-slate-500"><tr>{["Time", "Kind", "Stage", "Outcome", "Duration", "Work"].map((label) => <th key={label} className="px-4 py-3 font-semibold">{label}</th>)}</tr></thead><tbody className="divide-y divide-slate-100">{[...events].reverse().map((event) => <tr key={event.sequence} className="text-slate-700"><td className="px-4 py-3 text-slate-500">{time(event.capturedAt)}</td><td className="px-4 py-3 font-medium">{event.kind}</td><td className="px-4 py-3">{event.stage ?? "durability"}</td><td className={`px-4 py-3 font-semibold ${event.outcome === "success" ? "text-emerald-700" : "text-rose-700"}`}>{event.errorClass ?? event.outcome}</td><td className="px-4 py-3">{duration(event.durationNanos)}</td><td className="px-4 py-3 text-right">{event.kind === "query" ? `${count(event.documentsExamined)} examined / ${count(event.documentsReturned)} returned` : `${count(event.changes)} changes`}</td></tr>)}</tbody></table></div>;
}

export function EmptyState({ title, detail }: { title: string; detail: string }) { return <div className="rounded-xl border border-dashed border-slate-300 bg-white px-5 py-10 text-center shadow-sm"><p className="font-semibold text-slate-700">{title}</p><p className="mx-auto mt-2 max-w-md text-sm leading-6 text-slate-500">{detail}</p></div>; }

export function rows(sample: AdminSample | undefined, definitions: { label: string; path?: string; value?: () => string }[]): { label: string; value: string }[] { return definitions.map((definition) => ({ label: definition.label, value: definition.value ? definition.value() : String(valueAt(sample, definition.path ?? "") ?? "—") })); }

export function storageRows(sample: AdminSample | undefined) {
  const storage = object(valueAt(sample, "stats.storage"));
  const fence = object(valueAt(sample, "stats.primaryWriteFence"));
  return [
    { label: "Physical pages", value: count(storage.physicalPages) }, { label: "Physical generation", value: count(storage.generation) },
    { label: "Primary write fence", value: fence.enabled ? "enforced" : "not configured" }, { label: "Fence checks / rejected", value: `${count(fence.checks)} / ${count(fence.rejected)}` },
    { label: "Rollback protection", value: storage.rollbackProtectionEnabled ? "enabled" : "not configured" }, { label: "Rollback anchor", value: storage.rollbackAnchorHealthy ? "healthy" : "unavailable" },
    { label: "Anchor lag", value: `${count(storage.rollbackAnchorSequenceLag)} commits / ${count(storage.rollbackAnchorGenerationLag)} generations` }, { label: "Anchor failures", value: count(storage.rollbackAnchorFailures) },
    { label: "Anchor backend", value: String(storage.rollbackAnchorBackend ?? "—") }, { label: "Anchor latency / timeout", value: `${duration(storage.rollbackAnchorMaxLatencyNanos)} / ${duration(storage.rollbackAnchorTimeoutNanos)}` },
    { label: "Storage quota", value: `${bytes(storage.physicalBytes)} / ${bytes(storage.maxPhysicalBytes)}` }, { label: "Quota rejections", value: count(storage.limitRejections) },
    { label: "Reusable pages", value: count(storage.reusablePages) }, { label: "Tree splits / merges", value: `${count(storage.treeSplits)} / ${count(storage.treeMerges)}` },
    { label: "Persistent free map", value: storage.persistentFreeSpace ? "available" : "unavailable" }, { label: "Free-map load failures", value: count(storage.freeSpaceLoadFailures) },
    { label: "Retained commits", value: count(storage.retainedCommits) }, { label: "Retained history", value: bytes(storage.retainedCommitBytes) },
    { label: "Retention overage", value: `${count(storage.retainedCommitOverage)} / ${bytes(storage.retainedCommitByteOverage)}` },
    { label: "Page cache hit", value: percent(storage.pageCacheHitRatio, storage.pageCacheAvailable) }, { label: "Document cache hit", value: percent(storage.documentCacheHitRatio, storage.documentCacheAvailable) },
  ];
}

export function durabilityRows(sample: AdminSample | undefined) {
  const stats = object(valueAt(sample, "stats"));
  const recovery = object(stats.recovery);
  const limits = object(stats.resourceLimits);
  const maintenance = object(stats.maintenance);
  return [
    { label: "Commit max", value: duration(stats.commitMaxNanos) },
    { label: "Startup recovery", value: recovery.recovered ? "recovered" : "clean" }, { label: "Meta roots", value: `${count(recovery.validMetaRoots)} valid` }, { label: "Removed crash tails", value: bytes(recovery.removedTailBytes) },
    { label: "Document limit", value: bytes(limits.maxDocumentBytes) }, { label: "Transaction limit", value: bytes(limits.maxTransactionBytes) }, { label: "Index build limit", value: bytes(limits.maxIndexBuildBytes) }, { label: "Resource rejections", value: count(limits.rejections) },
    { label: "Write transactions", value: count(stats.writeTransactions) }, { label: "Write conflicts", value: count(stats.writeTransactionConflicts) }, { label: "Write aborts", value: count(stats.writeTransactionAborts) },
    { label: "Index builds", value: `${count(stats.indexBuildsActive)} active / ${count(stats.indexBuildsFailed)} failed` }, { label: "Durable index builds", value: `${count(stats.persistentIndexBuildsActive)} active` }, { label: "Last index build", value: duration(stats.indexBuildLastNanos) }, { label: "Max index build", value: duration(stats.indexBuildMaxNanos) },
    { label: "Rejected transactions", value: count(stats.rejectedTransactions) }, { label: "Reclaimable last scan", value: count(maintenance.reclaimablePages) }, { label: "Reclamation mode", value: maintenance.enabled ? "online" : "manual" }, { label: "Last scan attempts", value: count(maintenance.lastScanAttempts) }, { label: "Online conflicts", value: count(maintenance.conflicts) },
    { label: "Backup last size", value: bytes(stats.backupLastBytes) }, { label: "Backup last duration", value: duration(stats.backupLastNanos) }, { label: "Backup failures", value: count(stats.backupFailures) }, { label: "Telemetry drops", value: count(stats.telemetryDropped) },
  ];
}

export function healthTone(level: string): string { return level === "healthy" ? "bg-emerald-100 text-emerald-700" : level === "degraded" ? "bg-amber-100 text-amber-800" : level === "unavailable" ? "bg-slate-100 text-slate-600" : "bg-rose-100 text-rose-700"; }
function healthPanelTone(level: string): string { return level === "healthy" ? "border-emerald-200 bg-emerald-50/70" : level === "degraded" ? "border-amber-200 bg-amber-50/70" : level === "unavailable" ? "border-slate-200 bg-slate-50" : "border-rose-200 bg-rose-50/70"; }
function healthDot(level: string): string { return level === "healthy" ? "bg-emerald-500" : level === "degraded" ? "bg-amber-500" : level === "unavailable" ? "bg-slate-400" : "bg-rose-500"; }
function healthText(level: string): string { return level === "healthy" ? "text-emerald-700" : level === "degraded" ? "text-amber-800" : level === "unavailable" ? "text-slate-500" : "text-rose-700"; }

export function queryRows(sample: AdminSample | undefined) {
  const queries = object(valueAt(sample, "stats.queries"));
  return [{ label: "Index scans", value: count(queries.indexScans) }, { label: "Collection scans", value: count(queries.collectionScans) }, { label: "ID lookups", value: count(queries.idLookups) }, { label: "Examined / returned", value: ratio(queries.documentsExamined, queries.documentsReturned) }];
}

export function realtimeRows(sample: AdminSample | undefined) {
  const realtime = object(valueAt(sample, "stats.realtime"));
  return [{ label: "Shared views", value: count(realtime.sharedViews) }, { label: "Pending batches", value: `${count(realtime.pendingBatches)} / ${count(realtime.pendingBatchCapacity)}` }, { label: "Pending changes", value: `${count(realtime.pendingChanges)} / ${count(realtime.pendingChangeCapacity)}` }, { label: "Pending payload", value: `${bytes(realtime.pendingBytes)} / ${bytes(realtime.pendingByteCapacity)}` }, { label: "Watcher payload", value: `${bytes(realtime.watcherPendingBytes)} / ${bytes(realtime.watcherByteCapacity)}` }, { label: "Dispatch batches", value: `${count(realtime.dispatchPendingBatches)} / ${count(realtime.dispatchBatchCapacity)}` }, { label: "Dispatch changes", value: `${count(realtime.dispatchPendingChanges)} / ${count(realtime.dispatchChangeCapacity)}` }, { label: "Dispatch payload", value: `${bytes(realtime.dispatchPendingBytes)} / ${bytes(realtime.dispatchByteCapacity)}` }, { label: "Queue overflows", value: count(realtime.queueOverflows) }, { label: "Slow consumers", value: count(realtime.slowConsumers) }];
}

export function coordinatorRows(sample: AdminSample | undefined) {
  const coordinator = object(valueAt(sample, "stats.commitCoordinator"));
  return [{ label: "Pending admission", value: coordinator.enabled ? `${count(coordinator.pending)} / ${count(coordinator.pendingCapacity)}` : "not enabled" }, { label: "Admitted", value: count(coordinator.admitted) }, { label: "Queue rejected", value: count(coordinator.admissionRejected) }, { label: "Batches", value: count(coordinator.batches) }, { label: "Grouped requests", value: count(coordinator.groupedRequests) }, { label: "Outcome unknown", value: count(coordinator.outcomeUnknown) }];
}

export function transportRows(sample: AdminSample | undefined) {
  const server = object(sample?.server);
  return [{ label: "Requests / sec", value: rate(server.rpcRequestsPerSecond, true) }, { label: "Failures / sec", value: rate(server.rpcFailuresPerSecond, true) }, { label: "Realtime connections", value: count(server.realtimeConnections) }, { label: "Maximum latency", value: duration(server.rpcMaxNanos) }, { label: "Idempotency replays", value: count(server.rpcIdempotencyReplays) }, { label: "Outcome unknown", value: count(server.rpcIdempotencyOutcomeUnknown) }, { label: "Outbound limits", value: count(server.realtimeOutboundOverflows) }, { label: "Atomic commits", value: count(server.rpcAtomicCommits) }, { label: "Atomic rollbacks", value: count(server.rpcAtomicRollbacks) }, { label: "Method workers", value: count(server.workerConnections) }, { label: "Query publications", value: count(server.workerPublications) }, { label: "Policy denied", value: count(server.workerPolicyDenied) }, { label: "Policy invalidations", value: count(server.workerPolicyInvalidations) }, { label: "Worker protocol failures", value: count(server.workerProtocolFailures) }];
}

export function sampleMetrics(sample: AdminSample | undefined) {
  const stats = object(sample?.stats);
  const rates = object(sample?.rates);
  const storage = object(stats.storage);
  const realtime = object(stats.realtime);
  const queries = object(stats.queries);
  return { stats, rates, storage, realtime, queries, uptime: uptime(stats.uptimeNanos) };
}
