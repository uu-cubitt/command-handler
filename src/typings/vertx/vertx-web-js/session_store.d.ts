/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./session.d.ts" />

declare module "vertx-web-js/session_store" {
  export = SessionStore;
}

/**
 * A session store is used to store sessions for an Vert.x-Web web app
 */
interface SessionStore
{

  /**
   * The retry timeout value in milli seconds used by the session handler when it retrieves a value from the store.<p/>
 *
   * A non positive value means there is no retry at all.
   */
  retryTimeout(): number;

  /**
   * Create a new session
   */
  createSession(timeout: number): Session;

  /**
   * Get the session with the specified ID
   */
  get(id: string, resultHandler: (res: Session, err?: Throwable) => void): void;

  /**
   * Delete the session with the specified ID
   */
  delete(id: string, resultHandler: (res: boolean, err?: Throwable) => void): void;

  /**
   * Add a session with the specified ID
   */
  put(session: Session, resultHandler: (res: boolean, err?: Throwable) => void): void;

  /**
   * Remove all sessions from the store
   */
  clear(resultHandler: (res: boolean, err?: Throwable) => void): void;

  /**
   * Get the number of sessions in the store
   */
  size(resultHandler: (res: number, err?: Throwable) => void): void;

  /**
   * Close the store
   */
  close(): void;
}

declare var SessionStore: {
}
