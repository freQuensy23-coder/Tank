# Microsoft Foundry Decision-1

The 2026-10-25 operator request replaces Jev for Tank's crypto relevance filter and event router. Analyst/scout generation continues through the Codex subscription gateway.

## Configure before deployment

The deployment owner must securely add `TANK_FOUNDRY_API_KEY` to `/srv/tank-deploy/.env` before dispatching the manual Deploy workflow from `main`. This change does not edit that production file, deploy a service, or reuse Hermes's private secret file. Compose requires the key and fails before startup if it is absent. Keep it out of Git and output. Standalone runs also accept `FOUNDRY_API_KEY`; the prefixed name takes precedence.

Optional settings (defaults shown in `.env.example`):

- `TANK_FOUNDRY_ENDPOINT=https://alexeym-4644-resource.services.ai.azure.com`
- `TANK_FOUNDRY_DEPLOYMENT=decision-1`

The endpoint is the resource origin, not the project URL or OpenAI URL. The adapter accepts HTTPS Azure Foundry resource origins and sends the key only in the `api-key` header. Configure the deployment for **Microsoft-Decision-1 version 1**. The API request uses the deployment name; the response identifies `microsoft-decision-1` but does not attest a version. Verify deployment metadata when replacing it.

Run `tank doctor` to check credential presence. The key is excluded from Settings representations/serialization, redacted from logs and errors, and explicitly removed from replay settings. Agent sandboxes receive no Foundry credential.

## Request and results

`POST <resource>/providers/microsoft/v1/systemone` receives `{model, state, questions}`. Each filter/router question uses `type: noul`, instructions and true/false criteria. The response must contain the expected model, usage object and every requested answer as a finite numeric `noul` probability between 0 and 1. Strings, booleans, nonfinite values, missing answers and changed model identities fail the request. HTTP failures propagate; the adapter does not retry or switch providers.

Scores are cached by provider, resource, deployment, prompt version and exact question/state fingerprints. Jev cache entries cannot answer new Decision-1 requests. Usage retains requested and resolved model identities, endpoint and token counts. Missing monetary cost is unknown, not zero. Existing persisted `JEV_*` Ops stage codes remain stable, with neutral filter display labels so historical Jev outcomes retain their meaning.

The 0.5 filter/router cutoffs remain explicitly uncalibrated. Prior Jev scores and quality measurements do not validate this model. Use [offline calibration](OFFLINE_CALIBRATION.md) to propose model-specific cutoffs from labeled real news; no thresholds are automatically changed.

Official API guide: https://learn.microsoft.com/en-us/azure/foundry/foundry-models/how-to/use-foundry-models-microsoft-decision
