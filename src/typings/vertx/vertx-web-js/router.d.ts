/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./route.d.ts" />
/// <reference path="../vertx-js/http_server_request.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/router" {
  export = Router;
}

/**
 * A router receives request from an HttpServer and routes it to the first matching
 * Route that it contains. A router can contain many routes.
 * <p>
 * Routers are also used for routing failures.
 */
interface Router
{

  /**
   * This method is used to provide a request to the router. Usually you take request from the
   *  and pass it to this method. The
   * router then routes it to matching routes.
   */
  accept(request: HttpServerRequest): void;

  /**
   * Add a route with no matching criteria, i.e. it matches all requests or failures.
   */
  route(): Route;

  /**
   * Add a route that matches the specified HTTP method and path
   */
  route(method: any, path: string): Route;

  /**
   * Add a route that matches the specified path
   */
  route(path: string): Route;

  /**
   * Add a route that matches the specified HTTP method and path regex
   */
  routeWithRegex(method: any, regex: string): Route;

  /**
   * Add a route that matches the specified path regex
   */
  routeWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP GET request
   */
  get(): Route;

  /**
   * Add a route that matches a HTTP GET request and the specified path
   */
  get(path: string): Route;

  /**
   * Add a route that matches a HTTP GET request and the specified path regex
   */
  getWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP HEAD request
   */
  head(): Route;

  /**
   * Add a route that matches a HTTP HEAD request and the specified path
   */
  head(path: string): Route;

  /**
   * Add a route that matches a HTTP HEAD request and the specified path regex
   */
  headWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP OPTIONS request
   */
  options(): Route;

  /**
   * Add a route that matches a HTTP OPTIONS request and the specified path
   */
  options(path: string): Route;

  /**
   * Add a route that matches a HTTP OPTIONS request and the specified path regex
   */
  optionsWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP PUT request
   */
  put(): Route;

  /**
   * Add a route that matches a HTTP PUT request and the specified path
   */
  put(path: string): Route;

  /**
   * Add a route that matches a HTTP PUT request and the specified path regex
   */
  putWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP POST request
   */
  post(): Route;

  /**
   * Add a route that matches a HTTP POST request and the specified path
   */
  post(path: string): Route;

  /**
   * Add a route that matches a HTTP POST request and the specified path regex
   */
  postWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP DELETE request
   */
  delete(): Route;

  /**
   * Add a route that matches a HTTP DELETE request and the specified path
   */
  delete(path: string): Route;

  /**
   * Add a route that matches a HTTP DELETE request and the specified path regex
   */
  deleteWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP TRACE request
   */
  trace(): Route;

  /**
   * Add a route that matches a HTTP TRACE request and the specified path
   */
  trace(path: string): Route;

  /**
   * Add a route that matches a HTTP TRACE request and the specified path regex
   */
  traceWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP CONNECT request
   */
  connect(): Route;

  /**
   * Add a route that matches a HTTP CONNECT request and the specified path
   */
  connect(path: string): Route;

  /**
   * Add a route that matches a HTTP CONNECT request and the specified path regex
   */
  connectWithRegex(regex: string): Route;

  /**
   * Add a route that matches any HTTP PATCH request
   */
  patch(): Route;

  /**
   * Add a route that matches a HTTP PATCH request and the specified path
   */
  patch(path: string): Route;

  /**
   * Add a route that matches a HTTP PATCH request and the specified path regex
   */
  patchWithRegex(regex: string): Route;

  /**
   * @return a list of all the routes on this router
   */
  getRoutes(): Array<Route>;

  /**
   * Remove all the routes from this router
   */
  clear(): Router;

  /**
   * Mount a sub router on this router
   */
  mountSubRouter(mountPoint: string, subRouter: Router): Router;

  /**
   * Specify a handler for any unhandled exceptions on this router. The handler will be called for exceptions thrown
   * from handlers. This does not affect the normal failure routing logic.
   */
  exceptionHandler(exceptionHandler: (e: Throwable) => void): Router;

  /**
   * Used to route a context to the router. Used for sub-routers. You wouldn't normally call this method directly.
   */
  handleContext(context: RoutingContext): void;

  /**
   * Used to route a failure to the router. Used for sub-routers. You wouldn't normally call this method directly.
   */
  handleFailure(context: RoutingContext): void;
}

declare var Router: {

  /**
   * Create a router
   */
  router(vertx: Vertx): Router;
}
