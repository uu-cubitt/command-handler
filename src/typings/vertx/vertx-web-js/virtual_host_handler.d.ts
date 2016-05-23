/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/virtual_host_handler" {
  export = VirtualHostHandler;
}

/**
 * Handler that will filter requests based on the request Host name.
 */
interface VirtualHostHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var VirtualHostHandler: {

  /**
   * Create a handler
   */
  create(hostname: string, handler: (e: RoutingContext) => void): VirtualHostHandler;
}
