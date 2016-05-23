/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./route.d.ts" />
/// <reference path="./cookie.d.ts" />
/// <reference path="./locale.d.ts" />
/// <reference path="./file_upload.d.ts" />
/// <reference path="../vertx-js/http_server_request.d.ts" />
/// <reference path="./session.d.ts" />
/// <reference path="../vertx-auth-common-js/user.d.ts" />
/// <reference path="../vertx-js/buffer.d.ts" />
/// <reference path="../vertx-js/http_server_response.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-web-js/routing_context" {
  export = RoutingContext;
}

/**
 * Represents the context for the handling of a request in Vert.x-Web.
 * <p>
 * A new instance is created for each HTTP request that is received in the
 * accept of the router.
 * <p>
 * The same instance is passed to any matching request or failure handlers during the routing of the request or
 * failure.
 * <p>
 * The context provides access to the  and 
 * and allows you to maintain arbitrary data that lives for the lifetime of the context. Contexts are discarded once they
 * have been routed to the handler for the request.
 * <p>
 * The context also provides access to the Session, cookies and body for the request, given the correct handlers
 * in the application.
 */
interface RoutingContext
{

  /**
   * @return the HTTP request object
   */
  request(): HttpServerRequest;

  /**
   * @return the HTTP response object
   */
  response(): HttpServerResponse;

  /**
   * Tell the router to route this context to the next matching route (if any).
   * This method, if called, does not need to be called during the execution of the handler, it can be called
   * some arbitrary time later, if required.
   * <p>
   * If next is not called for a handler then the handler should make sure it ends the response or no response
   * will be sent.
   */
  next(): void;

  /**
   * Fail the context with the specified status code.
   * <p>
   * This will cause the router to route the context to any matching failure handlers for the request. If no failure handlers
   * match a default failure response will be sent.
   */
  fail(statusCode: number): void;

  /**
   * Fail the context with the specified throwable.
   * <p>
   * This will cause the router to route the context to any matching failure handlers for the request. If no failure handlers
   * match a default failure response with status code 500 will be sent.
   */
  fail(throwable: Throwable): void;

  /**
   * Put some arbitrary data in the context. This will be available in any handlers that receive the context.
   */
  put(key: string, obj: any): RoutingContext;

  /**
   * Get some data from the context. The data is available in any handlers that receive the context.
   */
  get(key: string): any;

  /**
   * @return the Vert.x instance associated to the initiating Router for this context
   */
  vertx(): Vertx;

  /**
   * @return the mount point for this router. It will be null for a top level router. For a sub-router it will be the path
   * at which the subrouter was mounted.
   */
  mountPoint(): string;

  /**
   * @return the current route this context is being routed through.
   */
  currentRoute(): Route;

  /**
   * Return the normalised path for the request.
   * <p>
   * The normalised path is where the URI path has been decoded, i.e. any unicode or other illegal URL characters that
   * were encoded in the original URL with `%` will be returned to their original form. E.g. `%20` will revert to a space.
   * Also `+` reverts to a space in a query.
   * <p>
   * The normalised path will also not contain any `..` character sequences to prevent resources being accessed outside
   * of the permitted area.
   * <p>
   * It's recommended to always use the normalised path as opposed to 
   * if accessing server resources requested by a client.
   */
  normalisedPath(): string;

  /**
   * Get the cookie with the specified name. The context must have first been routed to a CookieHandler
   * for this to work.
   */
  getCookie(name: string): Cookie;

  /**
   * Add a cookie. This will be sent back to the client in the response. The context must have first been routed
   * to a CookieHandler for this to work.
   */
  addCookie(cookie: Cookie): RoutingContext;

  /**
   * Remove a cookie. The context must have first been routed to a CookieHandler
   * for this to work.
   */
  removeCookie(name: string): Cookie;

  /**
   * @return the number of cookies. The context must have first been routed to a CookieHandler
   * for this to work.
   */
  cookieCount(): number;

  /**
   * @return a set of all the cookies. The context must have first been routed to a CookieHandler
   * for this to be populated.
   */
  cookies(): Array<Cookie>;

  /**
   * @return  the entire HTTP request body as a string, assuming UTF-8 encoding. The context must have first been routed to a
   * BodyHandler for this to be populated.
   */
  getBodyAsString(): string;

  /**
   * Get the entire HTTP request body as a string, assuming the specified encoding. The context must have first been routed to a
   * BodyHandler for this to be populated.
   */
  getBodyAsString(encoding: string): string;

  /**
   * @return Get the entire HTTP request body as a . The context must have first been routed to a
   * BodyHandler for this to be populated.
   */
  getBodyAsJson(): any;

  /**
   * @return Get the entire HTTP request body as a . The context must have first been routed to a
   * BodyHandler for this to be populated.
   */
  getBody(): Buffer;

  /**
   * @return a set of fileuploads (if any) for the request. The context must have first been routed to a
   * BodyHandler for this to work.
   */
  fileUploads(): Array<FileUpload>;

  /**
   * Get the session. The context must have first been routed to a SessionHandler
   * for this to be populated.
   * Sessions live for a browser session, and are maintained by session cookies.
   */
  session(): Session;

  /**
   * Get the authenticated user (if any). This will usually be injected by an auth handler if authentication if successful.
   */
  user(): User;

  /**
   * If the context is being routed to failure handlers after a failure has been triggered by calling
   * fail then this will return that throwable. It can be used by failure handlers to render a response,
   * e.g. create a failure response page.
   */
  failure(): Throwable;

  /**
   * If the context is being routed to failure handlers after a failure has been triggered by calling
   * fail  then this will return that status code.  It can be used by failure handlers to render a response,
   * e.g. create a failure response page.
   */
  statusCode(): number;

  /**
   * If the route specifies produces matches, e.g. produces `text/html` and `text/plain`, and the `accept` header
   * matches one or more of these then this returns the most acceptable match.
   */
  getAcceptableContentType(): string;

  /**
   * Add a handler that will be called just before headers are written to the response. This gives you a hook where
   * you can write any extra headers before the response has been written when it will be too late.
   */
  addHeadersEndHandler(handler: (e: void) => void): number;

  /**
   * Remove a headers end handler
   */
  removeHeadersEndHandler(handlerID: number): boolean;

  /**
   * Add a handler that will be called just before the response body has been completely written.
   * This gives you a hook where you can write any extra data to the response before it has ended when it will be too late.
   */
  addBodyEndHandler(handler: (e: void) => void): number;

  /**
   * Remove a body end handler
   */
  removeBodyEndHandler(handlerID: number): boolean;

  /**
   * @return true if the context is being routed to failure handlers.
   */
  failed(): boolean;

  /**
   * Set the body. Used by the BodyHandler. You will not normally call this method.
   */
  setBody(body: Buffer): void;

  /**
   * Set the session. Used by the SessionHandler. You will not normally call this method.
   */
  setSession(session: Session): void;

  /**
   * Set the user. Usually used by auth handlers to inject a User. You will not normally call this method.
   */
  setUser(user: User): void;

  /**
   * Clear the current user object in the context. This usually is used for implementing a log out feature, since the
   * current user is unbounded from the routing context.
   */
  clearUser(): void;

  /**
   * Set the acceptable content type. Used by
   */
  setAcceptableContentType(contentType: string): void;

  /**
   * Restarts the current router with a new path and reusing the original method. All path parameters are then parsed
   * and available on the params list.
   */
  reroute(path: string): void;

  /**
   * Restarts the current router with a new method and path. All path parameters are then parsed and available on the
   * params list.
   */
  reroute(method: any, path: string): void;

  /**
   * Returns the locales for the current request. The locales are determined from the `accept-languages` header and
   * sorted on quality.
 *
   * When 2 or more entries have the same quality then the order used to return the best match is based on the lowest
   * index on the original list. For example if a user has en-US and en-GB with same quality and this order the best
   * match will be en-US because it was declared as first entry by the client.
   */
  acceptableLocales(): Array<Locale>;

  /**
   * Helper to return the user preferred locale. It is the same action as returning the first element of the acceptable
   * locales.
   */
  preferredLocale(): Locale;
}

declare var RoutingContext: {
}
