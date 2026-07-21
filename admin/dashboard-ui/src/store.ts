import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";
import { loadDiagnostics, loadHistory, loadIndexCatalog, streamStats } from "./api";
import type { AdminSample, ConnectionState, DiagnosticEvent, IndexCatalogEntry } from "./types";
import { number, object } from "./utils";

type DashboardState = {
  token: string;
  rememberSession: boolean;
  hasHydrated: boolean;
  connection: ConnectionState;
  connectionLabel: string;
  error: string;
  samples: AdminSample[];
  diagnostics: DiagnosticEvent[];
  diagnosticsEnabled: boolean;
  diagnosticsStatus: string;
  indexes: IndexCatalogEntry[];
  indexesStatus: string;
  connect: (token: string, rememberSession?: boolean) => Promise<void>;
  disconnect: () => void;
  setHasHydrated: (hasHydrated: boolean) => void;
  refreshDiagnostics: () => Promise<void>;
};

let streamController: AbortController | null = null;
let diagnosticController: AbortController | null = null;
let diagnosticAfter = 0;
let diagnosticSession = "";
let diagnosticsPending = false;
const dashboardSessionKey = "meldbase.admin.session.v1";

function clearStoredSession(): void {
  try {
    window.sessionStorage.removeItem(dashboardSessionKey);
  } catch {
    // Browser storage can be unavailable in a privacy-restricted context.
  }
}

function diagnosticsEnabled(sample: AdminSample | undefined): boolean {
  return Boolean(object(sample?.stats).diagnostics && object(object(sample?.stats).diagnostics).enabled);
}

function diagnosticStatus(sample: AdminSample | undefined): string {
  const diagnostics = object(object(sample?.stats).diagnostics);
  if (!diagnostics.enabled) return "disabled";
  return `${number(diagnostics.retained).toLocaleString()} retained · ${number(diagnostics.overwritten).toLocaleString()} overwritten`;
}

function appendSample(samples: AdminSample[], sample: AdminSample): AdminSample[] {
  if (samples.at(-1)?.sequence === sample.sequence) return samples;
  return [...samples, sample].slice(-120);
}

export const useDashboardStore = create<DashboardState>()(persist((set, get) => ({
  token: "",
  rememberSession: false,
  hasHydrated: false,
  connection: "idle",
  connectionLabel: "Disconnected",
  error: "",
  samples: [],
  diagnostics: [],
  diagnosticsEnabled: false,
  diagnosticsStatus: "disabled",
  indexes: [],
  indexesStatus: "unavailable",
  async connect(token, rememberSession = get().rememberSession) {
    streamController?.abort();
    set({ connection: "connecting", connectionLabel: "Authenticating", error: "", token, rememberSession });
    try {
      const [history, catalog] = await Promise.all([
        loadHistory(token),
        loadIndexCatalog(token).catch(() => null),
      ]);
      const samples = history.samples.slice(-120);
      const latest = samples.at(-1);
      diagnosticAfter = 0;
      diagnosticSession = "";
      set({
        samples,
        diagnostics: [],
        diagnosticsEnabled: diagnosticsEnabled(latest),
        diagnosticsStatus: diagnosticStatus(latest),
        indexes: catalog?.indexes ?? [],
        indexesStatus: catalog ? `${catalog.indexes.length.toLocaleString()} published` : "unavailable",
        connection: "live",
        connectionLabel: "Live",
      });
      if (diagnosticsEnabled(latest)) void get().refreshDiagnostics();
      streamController = new AbortController();
      void streamStats(token, streamController.signal, (sample) => {
        const latestState = get();
        const nextSamples = appendSample(latestState.samples, sample);
        const enabled = diagnosticsEnabled(sample);
        set({
          samples: nextSamples,
          diagnosticsEnabled: enabled,
          diagnosticsStatus: diagnosticStatus(sample),
        });
        if (enabled) void get().refreshDiagnostics();
      }, (connection, delay) => set({
        connection,
        connectionLabel: connection === "live" ? "Live" : `Reconnecting in ${(delay ?? 0) / 1000}s`,
      }));
    } catch (error) {
      set({ token: "", rememberSession: false, connection: "error", connectionLabel: "Connection failed", error: error instanceof Error ? error.message : "Could not connect" });
      clearStoredSession();
    }
  },
  disconnect() {
    streamController?.abort();
    diagnosticController?.abort();
    streamController = null;
    diagnosticController = null;
    diagnosticAfter = 0;
    diagnosticSession = "";
    diagnosticsPending = false;
    set({
      token: "", rememberSession: false, connection: "idle", connectionLabel: "Disconnected", error: "", samples: [], diagnostics: [],
      diagnosticsEnabled: false, diagnosticsStatus: "disabled", indexes: [], indexesStatus: "unavailable",
    });
    clearStoredSession();
  },
  setHasHydrated(hasHydrated) { set({ hasHydrated }); },
  async refreshDiagnostics() {
    const { token, diagnosticsEnabled: enabled } = get();
    if (!token || !enabled || diagnosticsPending) return;
    diagnosticsPending = true;
    diagnosticController?.abort();
    diagnosticController = new AbortController();
    try {
      let pages = 0;
      let more = true;
      while (more && pages < 4) {
       pages += 1;
        const requestedAfter = diagnosticAfter;
        const snapshot = await loadDiagnostics(token, diagnosticAfter, diagnosticController.signal);
        if (!snapshot) {
          set({ diagnosticsStatus: "unavailable" });
          return;
        }
        if (snapshot.session && snapshot.session !== diagnosticSession) {
          const changed = diagnosticSession !== "";
          diagnosticSession = snapshot.session;
          diagnosticAfter = Math.max(0, number(object(snapshot.stats).recorded) - 256);
          set({ diagnostics: [] });
          if ((changed && requestedAfter !== 0) || diagnosticAfter !== 0) continue;
        }
        const existing = snapshot.truncated ? [] : get().diagnostics;
        const bySequence = new Map(existing.map((event) => [event.sequence, event]));
        for (const event of snapshot.events) {
          bySequence.set(event.sequence, event);
          diagnosticAfter = Math.max(diagnosticAfter, number(event.sequence));
        }
        set({ diagnostics: [...bySequence.values()].sort((left, right) => left.sequence - right.sequence).slice(-30) });
        more = Boolean(snapshot.hasMore && snapshot.events.length > 0);
      }
    } catch {
      if (!diagnosticController?.signal.aborted) set({ diagnosticsStatus: "temporarily unavailable" });
    } finally {
      diagnosticsPending = false;
    }
  },
}), {
  name: dashboardSessionKey,
 storage: createJSONStorage(() => sessionStorage),
  partialize: (state) => state.rememberSession && state.token ? { token: state.token, rememberSession: true } : {},
  onRehydrateStorage: () => (state) => state?.setHasHydrated(true),
}));
