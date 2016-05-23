/// <reference path="./throwable.d.ts" />

declare module "vertx-js/measured" {
  export = Measured;
}

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
interface Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;
}

declare var Measured: {
}
