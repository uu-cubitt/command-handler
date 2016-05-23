/// <reference path="./throwable.d.ts" />

declare module "vertx-js/srv_record" {
  export = SrvRecord;
}

/**
 * Represent a Service-Record (SRV) which was resolved for a domain.
 */
interface SrvRecord
{

  /**
   * Returns the priority for this service record.
   */
  priority(): number;

  /**
   * Returns the weight of this service record.
   */
  weight(): number;

  /**
   * Returns the port the service is running on.
   */
  port(): number;

  /**
   * Returns the name for the server being queried.
   */
  name(): string;

  /**
   * Returns the protocol for the service being queried (i.e. "_tcp").
   */
  protocol(): string;

  /**
   * Returns the service's name (i.e. "_http").
   */
  service(): string;

  /**
   * Returns the name of the host for the service.
   */
  target(): string;
}

declare var SrvRecord: {
}
