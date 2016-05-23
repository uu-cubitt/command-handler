/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./session_store.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/session_handler" {
  export = SessionHandler;
}

/**
 * A handler that maintains a Session for each browser session.
 * <p>
 * It looks up the session for each request based on a session cookie which contains a session ID. It stores the session
 * when the response is ended in the session store.
 * <p>
 * The session is available on the routing context with .
 * <p>
 * The session handler requires a CookieHandler to be on the routing chain before it.
 */
interface SessionHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;

  /**
   * Set the session timeout
   */
  setSessionTimeout(timeout: number): SessionHandler;

  /**
   * Set whether a nagging log warning should be written if the session handler is accessed over HTTP, not
   * HTTPS
   */
  setNagHttps(nag: boolean): SessionHandler;

  /**
   * Sets whether the 'secure' flag should be set for the session cookie. When set this flag instructs browsers to only
   * send the cookie over HTTPS. Note that this will probably stop your sessions working if used without HTTPS (e.g. in development).
   */
  setCookieSecureFlag(secure: boolean): SessionHandler;

  /**
   * Sets whether the 'HttpOnly' flag should be set for the session cookie. When set this flag instructs browsers to
   * prevent Javascript access to the the cookie. Used as a line of defence against the most common XSS attacks.
   */
  setCookieHttpOnlyFlag(httpOnly: boolean): SessionHandler;

  /**
   * Set the session cookie name
   */
  setSessionCookieName(sessionCookieName: string): SessionHandler;
}

declare var SessionHandler: {

  /**
   * Create a session handler
   */
  create(sessionStore: SessionStore): SessionHandler;
}
