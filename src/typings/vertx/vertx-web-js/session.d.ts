/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-web-js/session" {
  export = Session;
}

/**
 * Represents a browser session.
 * <p>
 * Sessions persist between HTTP requests for a single browser session. They are deleted when the browser is closed, or
 * they time-out. Session cookies are used to maintain sessions using a secure UUID.
 * <p>
 * Sessions can be used to maintain data for a browser session, e.g. a shopping basket.
 * <p>
 * The context must have first been routed to a SessionHandler
 * for sessions to be available.
 */
interface Session
{

  /**
   * @return The unique ID of the session. This is generated using a random secure UUID.
   */
  id(): string;

  /**
   * Put some data in a session
   */
  put(key: string, obj: any): Session;

  /**
   * Get some data from the session
   */
  get(key: string): any;

  /**
   * Remove some data from the session
   */
  remove(key: string): any;

  /**
   * @return the time the session was last accessed
   */
  lastAccessed(): number;

  /**
   * Destroy the session
   */
  destroy(): void;

  /**
   * @return has the session been destroyed?
   */
  isDestroyed(): boolean;

  /**
   * @return the amount of time in ms, after which the session will expire, if not accessed.
   */
  timeout(): number;

  /**
   * Mark the session as being accessed.
   */
  setAccessed(): void;
}

declare var Session: {
}
