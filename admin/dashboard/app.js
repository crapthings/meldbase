(() => {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const login = byId("login");
  const dashboard = byId("dashboard");
  const form = byId("login-form");
  const tokenInput = byId("token");
  const error = byId("login-error");
  const dot = byId("connection-dot");
  const connectionLabel = byId("connection-label");
  const samples = [];
	const diagnosticEvents = [];
  let token = "";
  let streamAbort = null;
	let diagnosticAfter = 0;
	let diagnosticSession = "";
	let diagnosticRefreshActive = false;

  const integer = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
  const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });

  function setConnection(state, label) {
    dot.className = `dot ${state}`;
    connectionLabel.textContent = label;
  }

  function text(id, value) {
    byId(id).textContent = value;
  }

  function number(value) {
    return integer.format(Number(value || 0));
  }

  function rate(value, valid) {
    return valid ? decimal.format(Number(value || 0)) : "warming up";
  }

  function percent(value, available) {
    return available ? `${decimal.format(Number(value || 0) * 100)}%` : "no samples";
  }

  function duration(nanos) {
    const value = Number(nanos || 0);
    if (value < 1_000) return `${integer.format(value)} ns`;
    if (value < 1_000_000) return `${decimal.format(value / 1_000)} µs`;
    if (value < 1_000_000_000) return `${decimal.format(value / 1_000_000)} ms`;
    return `${decimal.format(value / 1_000_000_000)} s`;
  }

  function bytes(value) {
    const amount = Number(value || 0);
    if (amount < 1024) return `${integer.format(amount)} B`;
    if (amount < 1024 * 1024) return `${decimal.format(amount / 1024)} KiB`;
    if (amount < 1024 * 1024 * 1024) return `${decimal.format(amount / (1024 * 1024))} MiB`;
    return `${decimal.format(amount / (1024 * 1024 * 1024))} GiB`;
  }

  function uptime(nanos) {
    let seconds = Math.floor(Number(nanos || 0) / 1_000_000_000);
    const days = Math.floor(seconds / 86400);
    seconds %= 86400;
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return days ? `${days}d ${hours}h` : hours ? `${hours}h ${minutes}m` : `${minutes}m`;
  }

  const healthSignalLabels = {
    databaseClosed: "database closed",
    writesDisabled: "writes fail-stopped",
    reactiveQueuePressure: "reactive queue pressure",
    reactiveQueueOverflow: "reactive queue overflow",
    slowConsumer: "slow consumer disconnected",
    persistentFreeSpaceDiscarded: "persistent free map discarded",
    commitRetentionPressure: "commit history exceeds its count or byte budget",
		indexBuildFailed: "a durable index build requires operator action",
		indexBuildRetentionPressure: "an index build is pinning commit history beyond its budget",
    storageQuotaExhausted: "physical storage quota is exhausted",
    storageLimitRejected: "a write exceeded the physical storage quota",
    durabilityFailure: "durability operation failed",
    telemetryDeliveryDropped: "telemetry delivery replaced",
    transportBusy: "transport concurrency busy",
    rpcOutcomeUnknown: "RPC outcome unknown",
    workerProtocolFailure: "worker protocol failure",
  };

  function renderHealth(health) {
    const level = ["healthy", "degraded", "critical"].includes(health?.overall) ? health.overall : "critical";
    const panel = byId("health-panel");
    panel.className = `health-panel panel ${level}`;
    text("health-overall", level);
    for (const component of ["database", "durability", "storage", "realtime", "telemetry", "transport"]) {
      text(`health-${component}`, health?.[component] || "unavailable");
    }
    const signals = Object.entries(health?.signals || {})
      .filter(([, active]) => active)
      .map(([name]) => healthSignalLabels[name] || name);
    text("health-signals", signals.length ? signals.join(" · ") : "No active engine health signals.");
  }

  function update(sample) {
    if (!sample || sample.version !== 10) return;
    const stats = sample.stats;
    const rates = sample.rates;
    const storage = stats.storage;
    const queries = stats.queries;
    const realtime = stats.realtime;
	const server = sample.server;
	const diagnosticStats = stats.diagnostics || {};
	renderHealth(sample.health);

    text("commit-sequence", number(stats.commitSequence));
    text("engine", `engine ${storage.engine || (stats.durable ? "v1" : "memory")}`);
    text("commit-rate", rate(rates.commitsPerSecond, rates.valid));
    text("change-rate", `${rate(rates.changesPerSecond, rates.valid)} changes / sec`);
    text("query-rate", rate(rates.queriesPerSecond, rates.valid));
    text("query-fail-rate", `${rate(rates.failedQueriesPerSecond, rates.valid)} failed / sec`);
    text("documents", number(stats.documents));
    text("collections", `${number(stats.collections)} collections`);

    text("active-cursors", `${number(queries.activeCursors)} active`);
    text("index-scans", number(queries.indexScans));
    text("collection-scans", number(queries.collectionScans));
    text("id-lookups", number(queries.idLookups));
    text("examined-ratio", queries.documentsReturned ? `${decimal.format(queries.documentsExamined / queries.documentsReturned)}×` : "no results");

    text("subscribers", `${number(realtime.querySubscribers)} subscribers`);
    text("shared-views", number(realtime.sharedViews));
    text("pending-batches", `${number(realtime.pendingBatches)} / ${number(realtime.pendingBatchCapacity)}`);
	text("pending-changes", `${number(realtime.pendingChanges)} / ${number(realtime.pendingChangeCapacity)}`);
    text("queue-overflows", number(realtime.queueOverflows));
    text("slow-consumers", number(realtime.slowConsumers));

    text("readers", `${number(storage.activeReaders)} readers`);
    text("physical-pages", number(storage.physicalPages));
	text("storage-quota", storage.storageMaxBytes ? `${bytes(storage.storageUsedBytes)} / ${bytes(storage.storageMaxBytes)}${storage.storageByteOverage ? ` · ${bytes(storage.storageByteOverage)} over` : ""}` : "not applicable");
	text("storage-limit-rejections", number(storage.storageLimitRejections));
    text("reusable-pages", number(storage.reusablePages));
	text("tree-splits", number(storage.treeSplits));
	text("tree-merges", number(storage.treeMerges));
	text("persistent-free-space", storage.persistentFreeSpace ? "active" : "inactive");
	text("free-space-load-failures", number(storage.freeSpaceLoadFailures));
	text("retained-commits", `${number(storage.retainedCommits)} / ${number(storage.commitRetentionMax)}`);
	text("retained-commit-bytes", `${bytes(storage.retainedCommitBytes)} / ${bytes(storage.commitRetentionMaxBytes)}`);
	text("retention-overage", storage.retentionPressure ? `${number(storage.commitRetentionOverage)} commits · ${bytes(storage.commitRetentionByteOverage)}` : "none");
    text("page-cache-hit", percent(rates.pageCacheHitRatio, storage.pageCache.hits + storage.pageCache.misses > 0));
    text("document-cache-hit", percent(rates.documentCacheHitRatio, storage.documentCache.hits + storage.documentCache.misses > 0));

    text("uptime", `${uptime(stats.uptimeNanos)} uptime`);
    text("commit-max", duration(storage.commitMaxLatencyNanos || stats.durability.walAppendMaxLatencyNanos));
	text("wal-size", `${bytes(stats.durability.walCurrentBytes)} · ${number(stats.durability.walCurrentCommits)} commits`);
	text("automatic-checkpoints", number(stats.durability.automaticCheckpoints));
	text("checkpoint-failures", number(stats.durability.checkpointFailures));
	text("checkpoint-max", duration(stats.durability.checkpointMaxLatencyNanos));
	const recovery = stats.recovery || {};
	text("recovery-status", recovery.recovered ? "recovered at startup" : (recovery.created ? "new database" : "clean open"));
	text("recovery-meta", `${number(recovery.rootValidMetaSlots)} / ${number(recovery.checksumValidMetaSlots)} root-valid${recovery.metaRedundancyDegraded ? " · degraded" : ""}`);
	text("recovery-tail", `${bytes(recovery.mainTailBytesRemoved)} main · ${bytes(recovery.walTailBytesRemoved)} WAL`);
	text("recovery-wal", `${number(recovery.walRecordsReplayed)} records`);
	const resources = stats.resources || { limits: {} };
	text("resource-document-limit", bytes(resources.limits.maxDocumentBytes));
	text("resource-transaction-limit", `${bytes(resources.limits.maxTransactionBytes)} · ${number(resources.limits.maxTransactionChanges)} changes`);
	text("resource-index-limit", `${bytes(resources.limits.maxIndexBuildBytes)} · ${number(resources.limits.maxIndexBuildEntries)} entries`);
	text("resource-rejections", number(resources.rejections));
	const transactions = stats.writeTransactions || {};
	text("write-transactions", `${number(transactions.active)} active · ${number(transactions.committed)} committed · ${number(transactions.noops)} no-op`);
	text("write-transaction-conflicts", number(transactions.conflicts));
	text("write-transaction-aborts", number(transactions.aborted));
	const indexBuilds = stats.indexBuilds || {};
	text("index-build-status", `${number(indexBuilds.active)} active · ${number(indexBuilds.completed)} completed · ${number(indexBuilds.failed)} failed · ${number(indexBuilds.conflicts)} conflicts`);
	text("index-build-persistent", `${number(indexBuilds.persistent)} unfinished · ${number(indexBuilds.scanning)} scan · ${number(indexBuilds.catchingUp)} catch-up · ${number(indexBuilds.ready)} ready · ${number(indexBuilds.persistentFailed)} failed · ${indexBuilds.retentionPressure ? "retention pressure" : indexBuilds.retentionLeaseActive ? "retention lease active" : "no retention lease"} · ${number(indexBuilds.persistentEntries)} entries · ${bytes(indexBuilds.persistentBytes)} · ${number(indexBuilds.schedulerYields)} scheduler yields`);
	text("index-build-last", `${number(indexBuilds.lastEntries)} entries · ${bytes(indexBuilds.lastBytes)} · ${duration(indexBuilds.lastDurationNanos)}`);
	text("index-build-max", duration(indexBuilds.maxDurationNanos));
    text("rejected-transactions", number(storage.rejectedTransactions));
    text("reclaimable-pages", number(stats.reclamation.lastReclaimable));
	text("reclamation-mode", stats.reclamation.lastOnline ? "online" : "blocking");
	text("reclamation-attempts", number(stats.reclamation.lastAttempts));
	text("reclamation-conflicts", number(stats.reclamation.conflicts));
	text("backup-last-size", bytes(stats.backup.lastBytes));
	text("backup-last-duration", duration(stats.backup.lastDurationNanos));
	text("backup-failures", number(stats.backup.failed));
    text("telemetry-drops", number(sample.sampler.droppedDeliveries));
	if (server) {
	  text("rpc-active", `${number(server.rpcActive)} active`);
	  text("rpc-rate", rate(rates.rpcRequestsPerSecond, rates.rpcRatesValid));
	  text("rpc-fail-rate", rate(rates.rpcFailuresPerSecond, rates.rpcRatesValid));
	  text("server-connections", number(server.activeConnections));
	  text("rpc-max", duration(server.rpcMaxLatencyNanos));
	  text("rpc-idempotency-replays", number(server.rpcIdempotencyReplays));
	  text("rpc-idempotency-unknown", number(server.rpcIdempotencyUnknown));
	  text("rpc-atomic-commits", number(server.rpcAtomicCommits));
	  text("rpc-atomic-rollbacks", number(server.rpcAtomicRollbacks));
	  text("worker-connections", `${number(server.worker?.connectedWorkers)} · ${number(server.worker?.registeredMethods)} methods`);
	  text("worker-publications", `${number(server.worker?.registeredPublications)} · ${number(server.worker?.policyActive)} active`);
	  text("worker-policy-denied", number(server.worker?.policyDenied));
	  text("worker-policy-invalidations", number(server.worker?.policyInvalidations));
	  text("worker-protocol-failures", number(server.worker?.protocolFailures));
	} else {
	  text("rpc-active", "unavailable");
	  text("rpc-rate", "—");
	  text("rpc-fail-rate", "—");
	  text("server-connections", "—");
	  text("rpc-max", "—");
	  text("rpc-idempotency-replays", "—");
	  text("rpc-idempotency-unknown", "—");
	  text("rpc-atomic-commits", "—");
	  text("rpc-atomic-rollbacks", "—");
	  text("worker-connections", "—");
	  text("worker-publications", "—");
	  text("worker-policy-denied", "—");
	  text("worker-policy-invalidations", "—");
	  text("worker-protocol-failures", "—");
	}
	text("diagnostic-status", diagnosticStats.enabled ? `${number(diagnosticStats.retained)} retained · ${number(diagnosticStats.overwritten)} overwritten` : "disabled");
    text("captured-at", `Sample ${number(sample.sequence)} · ${new Date(stats.capturedAt).toLocaleTimeString()}`);

    if (!samples.length || samples[samples.length - 1].sequence !== sample.sequence) {
      samples.push(sample);
      if (samples.length > 120) samples.shift();
      drawChart();
    }
	if (!dashboard.hidden && diagnosticStats.enabled) void refreshDiagnostics();
	if (!diagnosticStats.enabled) renderDiagnostics(false);
  }

  function drawChart() {
    const valid = samples.filter((sample) => sample.rates.valid);
    const width = 900;
    const height = 220;
    const padding = 8;
    const max = Math.max(1, ...valid.flatMap((sample) => [sample.rates.commitsPerSecond, sample.rates.queriesPerSecond]));
    const points = (field) => valid.map((sample, index) => {
      const x = valid.length < 2 ? width : (index / (valid.length - 1)) * width;
      const y = height - padding - (Number(sample.rates[field] || 0) / max) * (height - padding * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ");
    byId("commit-line").setAttribute("points", points("commitsPerSecond"));
    byId("query-line").setAttribute("points", points("queriesPerSecond"));
  }

  function drawGrid() {
    const group = document.querySelector(".grid-lines");
    for (let row = 1; row < 5; row += 1) {
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", "0");
      line.setAttribute("x2", "900");
      line.setAttribute("y1", String(row * 44));
      line.setAttribute("y2", String(row * 44));
      group.appendChild(line);
    }
  }

  async function request(path) {
    return fetch(path, { headers: { Authorization: `Bearer ${token}` }, cache: "no-store" });
  }

	async function refreshDiagnostics() {
	  if (diagnosticRefreshActive || !token) return;
	  diagnosticRefreshActive = true;
	  try {
		let more = true;
		let pages = 0;
		while (more && pages < 4) {
		  pages += 1;
		  const requestedAfter = diagnosticAfter;
		  const response = await request(`/v1/diagnostics?after=${requestedAfter}&limit=64`);
		  if (response.status === 404) {
			renderDiagnostics(false);
			return;
		  }
		  if (!response.ok) throw new Error(`Diagnostics returned ${response.status}`);
		  const snapshot = await response.json();
		  if (snapshot.version !== 1 || !Array.isArray(snapshot.events)) throw new Error("Unsupported diagnostics protocol");
		  const session = String(snapshot.session || "");
		  if (session && diagnosticSession !== session) {
			const replacing = diagnosticSession !== "";
			diagnosticSession = session;
			diagnosticEvents.length = 0;
			diagnosticAfter = Math.max(0, Number(snapshot.stats?.recorded || 0) - 256);
			if ((replacing && requestedAfter !== 0) || diagnosticAfter !== 0) continue;
		  }
		  if (snapshot.truncated) diagnosticEvents.length = 0;
		  for (const event of snapshot.events) {
			if (!diagnosticEvents.some((current) => current.sequence === event.sequence)) diagnosticEvents.push(event);
			diagnosticAfter = Math.max(diagnosticAfter, Number(event.sequence));
		  }
		  if (diagnosticEvents.length > 30) diagnosticEvents.splice(0, diagnosticEvents.length - 30);
		  more = snapshot.hasMore && snapshot.events.length > 0;
		}
		renderDiagnostics(true);
	  } catch {
		text("diagnostic-status", "temporarily unavailable");
	  } finally {
		diagnosticRefreshActive = false;
	  }
	}

	function renderDiagnostics(enabled) {
	  const table = byId("diagnostic-table");
	  const empty = byId("diagnostic-empty");
	  const rows = byId("diagnostic-rows");
	  rows.replaceChildren();
	  if (!enabled || diagnosticEvents.length === 0) {
		table.hidden = true;
		empty.hidden = false;
		empty.textContent = enabled ? "No slow, failed or sampled operations have been retained." : "Enable bounded diagnostics to see slow, failed or sampled operations.";
		return;
	  }
	  empty.hidden = true;
	  table.hidden = false;
	  for (const event of [...diagnosticEvents].reverse()) {
		const row = document.createElement("tr");
		const values = [
		  new Date(event.capturedAt).toLocaleTimeString(),
		  event.kind,
		  event.stage || "durability",
		  event.errorClass || event.outcome,
		  duration(event.durationNanos),
		  event.kind === "query"
			? `${number(event.documentsExamined)} examined / ${number(event.documentsReturned)} returned`
			: `${number(event.changes)} changes`,
		];
		values.forEach((value, index) => {
		  const cell = document.createElement("td");
		  cell.textContent = value;
		  if (index === 3) cell.className = `outcome-${event.outcome}`;
		  row.appendChild(cell);
		});
		rows.appendChild(row);
	  }
	}

  async function connect() {
    setConnection("idle", "Authenticating");
    const historyResponse = await request("/v1/stats/history");
    if (!historyResponse.ok) throw new Error(historyResponse.status === 401 ? "Invalid admin token" : `Admin API returned ${historyResponse.status}`);
    const history = await historyResponse.json();
    if (history.version !== 10 || !Array.isArray(history.samples)) throw new Error("Unsupported admin protocol");
    samples.length = 0;
    history.samples.forEach(update);
    login.hidden = true;
    dashboard.hidden = false;
    setConnection("live", "Live");
	const latest = history.samples[history.samples.length - 1];
	if (latest?.stats?.diagnostics?.enabled) await refreshDiagnostics();
    streamAbort = new AbortController();
    void stream(streamAbort.signal);
  }

  async function stream(signal) {
    let retryDelay = 500;
    while (!signal.aborted) {
      try {
        const response = await fetch("/v1/stats/stream", {
          headers: { Authorization: `Bearer ${token}`, Accept: "text/event-stream" },
          cache: "no-store",
          signal,
        });
        if (!response.ok || !response.body) throw new Error(`Stream returned ${response.status}`);
        setConnection("live", "Live");
        retryDelay = 500;
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { value, done } = await reader.read();
          if (done) throw new Error("Stream closed");
          buffer += decoder.decode(value, { stream: true });
          let boundary;
          while ((boundary = buffer.indexOf("\n\n")) >= 0) {
            const event = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            const data = event.split("\n").find((line) => line.startsWith("data: "));
            if (data) update(JSON.parse(data.slice(6)));
          }
        }
      } catch (streamError) {
        if (signal.aborted) return;
        setConnection("error", `Reconnecting in ${decimal.format(retryDelay / 1000)}s`);
        await abortableDelay(retryDelay, signal);
        retryDelay = Math.min(retryDelay * 2, 10_000);
      }
    }
  }

  function abortableDelay(milliseconds, signal) {
    return new Promise((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        window.clearTimeout(timeout);
        signal.removeEventListener("abort", finish);
        resolve();
      };
      const timeout = window.setTimeout(finish, milliseconds);
      signal.addEventListener("abort", finish, { once: true });
    });
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    error.textContent = "";
    token = tokenInput.value;
    tokenInput.value = "";
    try {
      await connect();
    } catch (connectError) {
      token = "";
      error.textContent = connectError.message;
      setConnection("error", "Connection failed");
    }
  });

  byId("disconnect").addEventListener("click", () => {
    if (streamAbort) streamAbort.abort();
    streamAbort = null;
    token = "";
    samples.length = 0;
	diagnosticEvents.length = 0;
	diagnosticAfter = 0;
	diagnosticSession = "";
	renderDiagnostics(false);
    dashboard.hidden = true;
    login.hidden = false;
    setConnection("idle", "Disconnected");
    tokenInput.focus();
  });

  drawGrid();
})();
