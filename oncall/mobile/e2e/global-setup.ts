import { api, mintToken } from "./helpers";

/**
 * Starts the load generator through the admin API so the live charts and a lag alert have something to show.
 * (The bad record for the DLQ scenario is injected later, after the generator is stopped and the pipeline has
 * drained, so the load generator cannot update or delete the injected row and change what the replay test sees.)
 */
export default async function globalSetup(): Promise<void> {
  const admin = mintToken("admin", "admin-user");
  const rate = Number(process.env.E2E_RATE ?? 40);
  const started = await api("POST", "/v1/generator", admin, { action: "start", rate }, { "Idempotency-Key": `e2e-start-${Date.now()}` });
  if (started.status !== 200 && started.status !== 409) throw new Error(`could not start the generator: ${started.status}`);
}
