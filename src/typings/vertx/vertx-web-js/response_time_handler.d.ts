/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/response_time_handler" {
  export = ResponseTimeHandler;
}

/**
 * Handler which adds a header `x-response-time` in the response of matching requests containing the time taken
 * in ms to process the request.
 */
interface ResponseTimeHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var ResponseTimeHandler: {

  /**
   * Create a handler
   */
  create(): ResponseTimeHandler;
}
