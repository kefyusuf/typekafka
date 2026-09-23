import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { defaultResource, resourceFromAttributes } from '@opentelemetry/resources';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { SEMRESATTRS_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import type { AppLogger } from './logger.js';

export interface Tracing {
  shutdown(): Promise<void>;
}

const noopTracing: Tracing = {
  shutdown: async () => {},
};

export function initTracing(options: {
  endpoint: string;
  serviceName?: string;
  logger?: AppLogger;
}): Tracing {
  const { endpoint, serviceName, logger } = options;

  if (!endpoint.trim()) {
    return noopTracing;
  }

  try {
    const sdk = new NodeSDK({
      resource: defaultResource().merge(
        resourceFromAttributes({
          [SEMRESATTRS_SERVICE_NAME]: serviceName || 'typekafka',
        }),
      ),
      traceExporter: new OTLPTraceExporter({ url: endpoint }),
    });
    sdk.start();
    logger?.info({ endpoint, serviceName }, 'OTel tracing enabled (OTLP HTTP exporter)');
    return { shutdown: () => sdk.shutdown() };
  } catch (error) {
    logger?.warn({ err: error }, 'OTel tracing init failed (disabled)');
    return noopTracing;
  }
}
