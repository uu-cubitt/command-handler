/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/favicon_handler" {
  export = FaviconHandler;
}

/**
 * A handler that serves favicons.
 * <p>
 * If no file system path is specified it will attempt to serve a resource called `favicon.ico` from the classpath.
 */
interface FaviconHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var FaviconHandler: {

  /**
   * Create a handler with defaults
   */
  create(): FaviconHandler;

  /**
   * Create a handler attempting to load favicon file from the specified path
   */
  create(path: string): FaviconHandler;

  /**
   * Create a handler attempting to load favicon file from the specified path, and with the specified max cache time
   */
  create(path: string, maxAgeSeconds: number): FaviconHandler;

  /**
   * Create a handler with the specified max cache time
   */
  create(maxAgeSeconds: number): FaviconHandler;
}
