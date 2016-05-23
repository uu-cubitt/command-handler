/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/cookie_handler" {
  export = CookieHandler;
}

/**
 * A handler which decodes cookies from the request, makes them available in the 
 * and writes them back in the response.
 */
interface CookieHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var CookieHandler: {

  /**
   * Create a cookie handler
   */
  create(): CookieHandler;
}
