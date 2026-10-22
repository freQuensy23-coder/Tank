// Node metrics recorder. Application series are Prometheus pull: an
// in-process prom-client registry exposed by `getPromText()` from /metrics.
// prom-client is lazy-imported so tests and CF builds don't drag it in.
//
// These series are not written to the OpenTelemetry MeterProvider. The
// tracer starts that provider only when OTEL_METRICS_EXPORTER is set
// (sdk-node 0.222 would otherwise default it to otlp). Call sites stay on
// this MetricsRecorder either way.

import type {
  MetricsRecorder,
  MetricTags,
  RecordableEvent,
} from "../types";

export interface NodeMetricsOptions {
  /** Prometheus default-bucket override for histograms (seconds). */
  histogramBuckets?: number[];
  /** When true, also mirror events into a Prometheus counter
   *  `oma_events_total{op,error_name}`. Default true. */
  mirrorEvents?: boolean;
}

export type NodeMetricsHandle = MetricsRecorder & {
  /** Prometheus exposition text for /metrics. Returns "" before the
   *  registry is ready or when prom-client is missing. */
  getPromText(): Promise<string>;
  /** Prometheus content-type ("text/plain; version=0.0.4; charset=utf-8"). */
  promContentType(): string;
};

const DEFAULT_BUCKETS = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
];

export async function createNodeMetricsRecorder(
  opts: NodeMetricsOptions = {},
): Promise<NodeMetricsHandle> {
  const promMod = await import("prom-client").catch(() => null);
  if (!promMod) {
    // Without prom-client we run noop counters. /metrics will return "".
    return {
      counter() {},
      histogram() {},
      gauge() {},
      recordEvent() {},
      async getPromText() { return ""; },
      promContentType() { return "text/plain; version=0.0.4"; },
    };
  }
  const prom = promMod;
  const registry = new prom.Registry();
  prom.collectDefaultMetrics({ register: registry });

  const buckets = opts.histogramBuckets ?? DEFAULT_BUCKETS;
  const mirrorEvents = opts.mirrorEvents ?? true;

  const counters = new Map<string, import("prom-client").Counter<string>>();
  const histograms = new Map<string, import("prom-client").Histogram<string>>();
  const gauges = new Map<string, import("prom-client").Gauge<string>>();

  // Pre-register the events mirror so it's always visible in /metrics, even
  // before the first event.
  if (mirrorEvents) {
    const c = new prom.Counter({
      name: "oma_events_total",
      help: "Count of recordEvent calls keyed by op and error_name.",
      labelNames: ["op", "error_name"],
      registers: [registry],
    });
    counters.set("oma_events_total", c);
  }

  function getCounter(name: string, tags?: MetricTags) {
    let c = counters.get(name);
    if (!c) {
      c = new prom.Counter({
        name,
        help: `OMA counter ${name}`,
        labelNames: tags ? Object.keys(tags) : [],
        registers: [registry],
      });
      counters.set(name, c);
    }
    return c;
  }
  function getHistogram(name: string, tags?: MetricTags) {
    let h = histograms.get(name);
    if (!h) {
      h = new prom.Histogram({
        name,
        help: `OMA histogram ${name}`,
        labelNames: tags ? Object.keys(tags) : [],
        buckets,
        registers: [registry],
      });
      histograms.set(name, h);
    }
    return h;
  }
  function getGauge(name: string, tags?: MetricTags) {
    let g = gauges.get(name);
    if (!g) {
      g = new prom.Gauge({
        name,
        help: `OMA gauge ${name}`,
        labelNames: tags ? Object.keys(tags) : [],
        registers: [registry],
      });
      gauges.set(name, g);
    }
    return g;
  }

  return {
    counter(name, value = 1, tags) {
      const c = getCounter(name, tags);
      if (tags) c.inc(stringifyTags(tags), value);
      else c.inc(value);
    },
    histogram(name, value, tags) {
      const h = getHistogram(name, tags);
      if (tags) h.observe(stringifyTags(tags), value);
      else h.observe(value);
    },
    gauge(name, value, tags) {
      const g = getGauge(name, tags);
      if (tags) g.set(stringifyTags(tags), value);
      else g.set(value);
    },
    recordEvent(event: RecordableEvent) {
      if (!mirrorEvents) return;
      const c = counters.get("oma_events_total");
      if (!c) return;
      c.inc({ op: event.op, error_name: event.error_name ?? "" }, 1);
    },
    async getPromText() {
      return registry.metrics();
    },
    promContentType() {
      return registry.contentType ?? "text/plain; version=0.0.4";
    },
  };
}

function stringifyTags(tags: MetricTags): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(tags)) out[k] = String(v);
  return out;
}
