import { MeldbaseClient } from "@meldbase/client";

// Check the application's proxy and identity path without writing database records.
export async function checkStarter(url, { fetch: fetcher = globalThis.fetch } = {}) {
  let step = "session";
  const request = async (input, init = {}) => {
    const headers = new Headers(init.headers);
    headers.set("origin", url);
    headers.set("sec-fetch-site", "same-origin");
    return fetcher(input, { ...init, headers, signal: AbortSignal.timeout(5_000) });
  };
  let client;
  try {
    const session = await request(`${url}/session`);
    if (!session.ok) throw new Error(`HTTP ${session.status}`);
    const { accessToken } = await session.json();
    if (typeof accessToken !== "string" || !accessToken) throw new Error("missing access token");
    step = "collection query";
    client = new MeldbaseClient({ baseUrl: `${url}/api`, accessToken: () => accessToken, fetch: request });
    await client.collection("todos").find({}, { limit: 1 }).fetch();
    step = "realtime ticket";
    const response = await request(`${url}/api/v1/realtime/tickets`, {
      method: "POST",
      headers: { authorization: `Bearer ${accessToken}` },
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const ticket = await response.json();
    if (typeof ticket.ticket !== "string" || !ticket.ticket) throw new Error("missing ticket");
    if (ticket.url !== `${url.replace(/^http/, "ws")}/v1/realtime`)
      throw new Error("realtime URL does not match the application proxy");
    return ["session", "collection query", "realtime ticket"];
  } catch (error) {
    const hints = {
      session: "Check the /session route and proxy Host; preserve Host with changeOrigin: false.",
      "collection query":
        "Check raw signing-key bytes (no trim), issuer, audience, expiry, workspace claim and todos access policy.",
      "realtime ticket": "Check realtime origin permissions and --public-realtime-url.",
    };
    // Never include response bodies, tokens, keys, or arbitrary upstream errors.
    const status = error.status ?? /^HTTP (\d{3})$/.exec(error.message)?.[1];
    throw new Error(`Startup check failed: ${step}${status ? ` (HTTP ${status})` : ""}. ${hints[step]}`);
  } finally {
    client?.close();
  }
}
