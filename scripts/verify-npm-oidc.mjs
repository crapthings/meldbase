import assert from "node:assert/strict";

// Check the same GitHub -> npm exchange used by npm CLI, without publishing a version.
// Reference: https://github.com/npm/cli/blob/latest/lib/utils/oidc.js
assert.equal(process.env.GITHUB_ACTIONS, "true", "OIDC verification requires GitHub Actions");
assert.equal(process.env.GITHUB_REPOSITORY, "crapthings/meldbase");
assert.equal(process.env.GITHUB_REF, "refs/heads/main");
assert(!process.env.NODE_AUTH_TOKEN, "OIDC verification must not receive a bootstrap token");
assert(!process.env.NPM_ID_TOKEN, "Request a fresh identity from this workflow");
assert(process.env.ACTIONS_ID_TOKEN_REQUEST_URL && process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN);

const identityURL = new URL(process.env.ACTIONS_ID_TOKEN_REQUEST_URL);
assert.equal(identityURL.protocol, "https:");
identityURL.searchParams.set("audience", "npm:registry.npmjs.org");
const identityResponse = await fetch(identityURL, {
  headers: { authorization: `Bearer ${process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN}`, accept: "application/json" },
  redirect: "error",
  signal: AbortSignal.timeout(30_000),
});
assert(identityResponse.ok, `GitHub identity request failed (HTTP ${identityResponse.status})`);
const identity = await identityResponse.json();
assert.equal(typeof identity.value, "string", "GitHub did not provide an identity token");
for (const name of ["@meldbase/client", "@meldbase/worker", "@meldbase/react"]) {
  const response = await fetch(
    `https://registry.npmjs.org/-/npm/v1/oidc/token/exchange/package/${name.replace("/", "%2f")}`,
    {
      method: "POST",
      headers: { authorization: `Bearer ${identity.value}`, accept: "application/json" },
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
    },
  );
  // Never log the response body: it contains a short-lived publishing credential.
  assert(response.ok, `${name}: OIDC exchange failed (HTTP ${response.status}); check the package trusted publisher`);
  const result = await response.json();
  assert.equal(typeof result.token, "string", `${name}: npm did not return an exchange token`);
  assert(result.token.length > 0);
  console.log(`${name}: GitHub workflow identity accepted by npm; no version published`);
}
