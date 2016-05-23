/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/route" {
  export = Route;
}

/**
 * A route is a holder for a set of criteria which determine whether an HTTP request or failure should be routed
 * to a handler.
 */
interface Route
{

  /**
   * Add an HTTP method for this route. By default a route will match all HTTP methods. If any are specified then the route
   * will only match any of the specified methods
   */
  method(method: any): Route;

  /**
   * Set the path prefix for this route. If set then this route will only match request URI paths which start with this
   * path prefix. Only a single path or path regex can be set for a route.
   */
  path(path: string): Route;

  /**
   * Set the path prefix as a regular expression. If set then this route will only match request URI paths, the beginning
   * of which match the regex. Only a single path or path regex can be set for a route.
   */
  pathRegex(path: string): Route;

  /**
   * Add a content type produced by this route. Used for content based routing.
   */
  produces(contentType: string): Route;

  /**
   * Add a content type consumed by this route. Used for content based routing.
   */
  consumes(contentType: string): Route;

  /**
   * Specify the order for this route. The router tests routes in that order.
   */
  order(order: number): Route;

  /**
   * Specify this is the last route for the router.
   */
  last(): Route;

  /**
   * Specify a request handler for the route. The router routes requests to handlers depending on whether the various
   * criteria such as method, path, etc match. There can be only one request handler for a route. If you set this more
   * than once it will overwrite the previous handler.
   */
  handler(requestHandler: (e: RoutingContext) => void): Route;

  /**
   * Like blockingHandler called with ordered = true
   */
  blockingHandler(requestHandler: (e: RoutingContext) => void): Route;

  /**
   * Specify a blocking request handler for the route.
   * This method works just like handler excepted that it will run the blocking handler on a worker thread
   * so that it won't block the event loop. Note that it's safe to call context.next() from the
   * blocking handler as it will be executed on the event loop context (and not on the worker thread.
 *
   * If the blocking handler is ordered it means that any blocking handlers for the same context are never executed
   * concurrently but always in the order they were called. The default value of ordered is true. If you do not want this
   * behaviour and don't mind if your blocking handlers are executed in parallel you can set ordered to false.
   */
  blockingHandler(requestHandler: (e: RoutingContext) => void, ordered: boolean): Route;

  /**
   * Specify a failure handler for the route. The router routes failures to failurehandlers depending on whether the various
   * criteria such as method, path, etc match. There can be only one failure handler for a route. If you set this more
   * than once it will overwrite the previous handler.
   */
  failureHandler(failureHandler: (e: RoutingContext) => void): Route;

  /**
   * Remove this route from the router
   */
  remove(): Route;

  /**
   * Disable this route. While disabled the router will not route any requests or failures to it.
   */
  disable(): Route;

  /**
   * Enable this route.
   */
  enable(): Route;

  /**
   * If true then the normalised request path will be used when routing (e.g. removing duplicate /)
   * Default is true
   */
  useNormalisedPath(useNormalisedPath: boolean): Route;

  /**
   * @return the path prefix (if any) for this route
   */
  getPath(): string;
}

declare var Route: {
}
