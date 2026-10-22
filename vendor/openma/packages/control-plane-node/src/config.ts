// The control plane's settings contract: plain values only (paths, ports,
// limits, credentials). A distribution builds a NodeConfig however it likes —
// the OMA default parses the environment once in
// @open-managed-agents/main-node/config (loadNodeConfig) — and hands it to
// createNodeControlPlane() inside NodeComponents. The control plane never
// reads the environment itself.

export type NodeEnvironment = Readonly<Record<string, string | undefined>>;

export type NodeProcessMode = "standalone" | "serverless";

/**
 * How live Session events reach SSE subscribers attached to other replicas.
 *
 *  - `memory`:   in-process only. Correct for a single replica (SQLite).
 *  - `pg-notify`: Postgres LISTEN/NOTIFY for the legacy `/v1/oma` stream,
 *                plus SQL tailing for the official `/v1/sessions` stream.
 *  - `sql-poll`: SQL tailing for both streams. Works on any shared SQL
 *                database; the default for MySQL.
 */
export type RealtimeFanoutMode = "memory" | "pg-notify" | "sql-poll";

export interface RealtimeFanoutConfig {
  mode: RealtimeFanoutMode;
  pollIntervalMs: number;
}

export type DatabaseConfig =
  | { kind: "sqlite"; path: string }
  | { kind: "postgres"; url: string }
  | { kind: "mysql"; url: string };

export type BlobBackendConfig =
  | { kind: "localfs"; dir: string }
  | {
      kind: "s3";
      endpoint: string;
      bucket: string;
      accessKey: string;
      secretKey: string;
      region: string;
      prefix?: string;
      forcePathStyle?: boolean;
      requestChecksumCalculation?: "WHEN_REQUIRED" | "WHEN_SUPPORTED";
      conditionalCreate?: "if-none-match" | "oss-forbid-overwrite";
    };

export interface NodeConfig {
  processMode: NodeProcessMode;
  database: DatabaseConfig;
  auth: {
    disabled: boolean;
    /** Better Auth's SQLite file; unused for Postgres/MySQL, which share the main database. */
    databasePath: string;
    /** Missing means "generate a per-process secret and warn", as before. */
    secret: string | undefined;
    cookieDomain: string | undefined;
    requireEmailVerify: boolean;
    google: { clientId: string | undefined; clientSecret: string | undefined };
    github: { clientId: string | undefined; clientSecret: string | undefined };
  };
  /** Root key for platform-owned ciphers (model cards, integrations). */
  platformRootSecret: string | undefined;
  blobs: {
    memory: BlobBackendConfig & { pollIntervalMs?: number };
    files: BlobBackendConfig;
  };
  /** MEMORY_QUEUE: "disabled" turns off the SQL-backed memory ingestion queue. */
  memoryQueue: "auto" | "disabled";
  realtime: RealtimeFanoutConfig;
  model: {
    apiKey: string | undefined;
    baseUrl: string | undefined;
    customHeaders: Record<string, string> | undefined;
  };
  dreamCurator: "model" | "dedup";
  execution: { ownerId: string; concurrency: number };
  workspace: {
    strategy: "ephemeral" | "checkpoint_restore";
    /** Target between safe turn checkpoints; no fixed wall-clock RPO. */
    checkpointIntervalMs: number;
  };
  managedWebhooks: {
    url: string | undefined;
    signingKey: string | undefined;
    organizationId: string | undefined;
  };
  paths: { sandboxWorkdir: string; sessionOutputs: string };
  http: {
    host: string;
    port: number;
    publicBaseUrl: string | undefined;
    gatewayOrigin: string;
    consoleDir: string | undefined;
    /** CONSOLE_BRANDING_DIR: branding.json plus the logo, favicon, and stylesheet it names. */
    brandingDir: string | undefined;
    metricsToken: string | undefined;
    /** Legacy static API key accepted alongside per-tenant keys. */
    apiKey: string | undefined;
  };
  tunnels: { domainSuffix: string };
  skillSources: {
    /**
     * SKILL_SOURCES_ALLOW_HOSTS: hosts the control plane may fetch skill
     * sources from even when they resolve to private addresses (and over
     * plain http). Entries are host names, IP literals, or `*.example.com`.
     */
    allowHosts: string[];
  };
  integrationsInternalToken: string | undefined;
  email: {
    host: string;
    port: number;
    secure: boolean;
    user: string | undefined;
    pass: string | undefined;
    fromAddress: string | undefined;
  } | null;
  feishuWsRunner: boolean;
  cron: {
    evalTick: string;
    memoryRetention: string;
    webhookEventsRetention: string;
    linearDispatch: string;
  };
  sandbox: {
    /**
     * The provider's own configuration namespace (SANDBOX_PROVIDER, E2B_*,
     * DAYTONA_*, …). Opaque to the control plane: it is handed to the
     * selected SandboxFactory, whose contract is env-shaped.
     */
    environment: NodeEnvironment;
  };
}

/**
 * The effective configuration with every secret removed, for startup logs
 * and diagnostics. Credentials inside URLs are masked; the sandbox provider's
 * namespace is dropped entirely because the control plane cannot know which
 * of its keys are secrets.
 */
export function redactNodeConfig(config: NodeConfig): Record<string, unknown> {
  const mask = (value: string | undefined): string | undefined => (value === undefined ? undefined : "***");
  const maskUrl = (url: string): string => {
    try {
      const parsed = new URL(url);
      if (parsed.password) parsed.password = "***";
      return parsed.toString();
    } catch {
      return "***";
    }
  };
  const blob = (backend: BlobBackendConfig & { pollIntervalMs?: number }) =>
    backend.kind === "localfs"
      ? backend
      : { ...backend, accessKey: mask(backend.accessKey), secretKey: mask(backend.secretKey) };
  const { sandbox: _sandbox, ...rest } = config;
  return {
    ...rest,
    database: config.database.kind === "sqlite" ? config.database : { ...config.database, url: maskUrl(config.database.url) },
    auth: {
      ...config.auth,
      secret: mask(config.auth.secret),
      google: { ...config.auth.google, clientSecret: mask(config.auth.google.clientSecret) },
      github: { ...config.auth.github, clientSecret: mask(config.auth.github.clientSecret) },
    },
    platformRootSecret: mask(config.platformRootSecret),
    blobs: { memory: blob(config.blobs.memory), files: blob(config.blobs.files) },
    model: { ...config.model, apiKey: mask(config.model.apiKey) },
    managedWebhooks: { ...config.managedWebhooks, signingKey: mask(config.managedWebhooks.signingKey) },
    http: { ...config.http, metricsToken: mask(config.http.metricsToken), apiKey: mask(config.http.apiKey) },
    integrationsInternalToken: mask(config.integrationsInternalToken),
    email: config.email === null ? null : { ...config.email, pass: mask(config.email.pass) },
  };
}
