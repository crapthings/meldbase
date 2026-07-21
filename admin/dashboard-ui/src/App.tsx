import { useEffect, useState, type FormEvent } from "react";
import { createHashRouter, RouterProvider } from "react-router-dom";
import { AppShell, StatusDot } from "./components";
import { DiagnosticsPage, EmptyRoute, IndexesPage, OverviewPage, RealtimePage, StoragePage, TransportPage } from "./pages";
import { useDashboardStore } from "./store";

const router = createHashRouter([
  { element: <AppShell />, children: [
    { path: "/", element: <OverviewPage /> },
    { path: "/storage", element: <StoragePage /> },
    { path: "/indexes", element: <IndexesPage /> },
    { path: "/realtime", element: <RealtimePage /> },
    { path: "/transport", element: <TransportPage /> },
    { path: "/diagnostics", element: <DiagnosticsPage /> },
    { path: "*", element: <EmptyRoute /> },
  ] },
]);

function LoginGate() {
  const [token, setToken] = useState("");
  const [rememberSession, setRememberSession] = useState(false);
  const connect = useDashboardStore((state) => state.connect);
  const error = useDashboardStore((state) => state.error);
  const connection = useDashboardStore((state) => state.connection);
  async function submit(event: FormEvent<HTMLFormElement>) { event.preventDefault(); await connect(token, rememberSession); setToken(""); }
  return <main className="grid min-h-screen place-items-center bg-slate-100 p-5 text-slate-950"><section className="w-full max-w-lg rounded-2xl border border-slate-200 bg-white p-7 shadow-xl shadow-slate-300/40 sm:p-9"><div className="flex items-start justify-between"><div><p className="text-[10px] font-semibold uppercase tracking-[.18em] text-indigo-600">Meldbase Observatory</p><h1 className="mt-2 text-2xl font-semibold tracking-tight text-slate-950">Operator console</h1><p className="mt-3 max-w-md text-sm leading-6 text-slate-600">Connect to this local process to inspect health, storage and telemetry. By default, your token remains only in memory.</p></div><StatusDot state={connection} /></div><form className="mt-8" onSubmit={submit}><label className="text-xs font-semibold text-slate-700" htmlFor="token">Admin bearer token</label><input id="token" value={token} onChange={(event) => setToken(event.target.value)} type="password" autoComplete="off" minLength={32} required placeholder="32+ byte admin token" className="mt-2 w-full rounded-lg border border-slate-300 bg-white px-3 py-3 text-sm text-slate-950 outline-none placeholder:text-slate-400 focus:border-indigo-500 focus:ring-4 focus:ring-indigo-100" /><label className="mt-4 flex cursor-pointer items-start gap-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-600"><input checked={rememberSession} onChange={(event) => setRememberSession(event.target.checked)} type="checkbox" className="mt-0.5 size-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" /><span><span className="block font-semibold text-slate-700">Remember in this tab</span><span className="mt-0.5 block text-xs leading-5 text-slate-500">Keeps the token in session storage only until this tab closes. Disconnect removes it immediately.</span></span></label><button disabled={connection === "connecting"} className="mt-3 w-full rounded-lg bg-indigo-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-indigo-500 disabled:cursor-wait disabled:opacity-60">{connection === "connecting" ? "Authenticating…" : "Connect to process"}</button>{error && <p role="alert" className="mt-3 text-sm text-rose-700">{error}</p>}</form></section></main>;
}

function RestoringSession() {
  return <main className="grid min-h-screen place-items-center bg-slate-100 p-5 text-slate-950"><p className="rounded-lg border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-600 shadow-sm">Restoring this tab&apos;s admin session…</p></main>;
}

export function App() {
  const token = useDashboardStore((state) => state.token);
  const connection = useDashboardStore((state) => state.connection);
  const hasHydrated = useDashboardStore((state) => state.hasHydrated);
  const connect = useDashboardStore((state) => state.connect);
  useEffect(() => {
    if (hasHydrated && token && connection === "idle") void connect(token);
  }, [connect, connection, hasHydrated, token]);
  if (!hasHydrated || (token && connection === "idle")) return <RestoringSession />;
  return token ? <RouterProvider router={router} /> : <LoginGate />;
}
