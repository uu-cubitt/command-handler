/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/cors_handler" {
  export = CorsHandler;
}

/**
 * A handler which implements server side http://www.w3.org/TR/cors/[CORS] support for Vert.x-Web.
 */
interface CorsHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;

  /**
   * Add an allowed method
   */
  allowedMethod(method: any): CorsHandler;

  /**
   * Add an allowed header
   */
  allowedHeader(headerName: string): CorsHandler;

  /**
   * Add a set of allowed headers
   */
  allowedHeaders(headerNames: Array<string>): CorsHandler;

  /**
   * Add an exposed header
   */
  exposedHeader(headerName: string): CorsHandler;

  /**
   * Add a set of exposed headers
   */
  exposedHeaders(headerNames: Array<string>): CorsHandler;

  /**
   * Set whether credentials are allowed
   */
  allowCredentials(allow: boolean): CorsHandler;

  /**
   * Set how long the browser should cache the information
   */
  maxAgeSeconds(maxAgeSeconds: number): CorsHandler;
}

declare var CorsHandler: {

  /**
   * Create a CORS handler
   */
  create(allowedOriginPattern: string): CorsHandler;
}
