/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/logger_handler" {
  export = LoggerHandler;
}

/**
 * A handler which logs request information to the Vert.x logger.
 */
interface LoggerHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var LoggerHandler: {

  /**
   * Create a handler with default format
   */
  create(): LoggerHandler;

  /**
   * Create a handler with he specified format
   */
  create(format: any): LoggerHandler;

  /**
   * Create a handler with he specified format
   */
  create(immediate: boolean, format: any): LoggerHandler;
}
