/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/measured.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-dropwizard-js/metrics_service" {
  export = MetricsService;
}

/**
 * The metrics service mainly allows to return a snapshot of measured objects.
 */
interface MetricsService
{

  /**
   * @param measured the measure object
   */
  getBaseName(measured: Measured): string;

  /**
   * @return the known metrics names by this service
   */
  metricsNames(): Array<string>;

  /**
   * Will return the metrics that correspond with the <code>measured</code> object, null if no metrics is available.<p/>
 *
   * Note: in the case of scaled servers, the JsonObject returns an aggregation of the metrics as the
   * dropwizard backend reports to a single server.
   */
  getMetricsSnapshot(measured: Measured): any;

  /**
   * Will return the metrics that begins with the <code>baseName</code>, null if no metrics is available.<p/>
 *
   * Note: in the case of scaled servers, the JsonObject returns an aggregation of the metrics as the
   * dropwizard backend reports to a single server.
   */
  getMetricsSnapshot(baseName: string): any;
}

declare var MetricsService: {

  /**
   * Creates a metric service for a given Vertx instance.
   */
  create(vertx: Vertx): MetricsService;
}
