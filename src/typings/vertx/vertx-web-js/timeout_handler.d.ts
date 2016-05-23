/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/timeout_handler" {
  export = TimeoutHandler;
}

/**
 * Handler that will timeout requests if the response has not been written after a certain time.
 * Timeout requests will be ended with an HTTP status code `408`.
 */
interface TimeoutHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var TimeoutHandler: {

  /**
   * Create a handler
   */
  create(): TimeoutHandler;

  /**
   * Create a handler
   */
  create(timeout: number): TimeoutHandler;
}
