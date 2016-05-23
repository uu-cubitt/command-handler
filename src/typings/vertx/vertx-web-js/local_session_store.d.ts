/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./session_store.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-web-js/local_session_store" {
  export = LocalSessionStore;
}

/**
 * A session store which is only available on a single node.
 * <p>
 * Can be used when sticky sessions are being used.
 */
interface LocalSessionStore
  extends SessionStore
{
}

declare var LocalSessionStore: {

  /**
   * Create a session store
   */
  create(vertx: Vertx): LocalSessionStore;

  /**
   * Create a session store
   */
  create(vertx: Vertx, sessionMapName: string): LocalSessionStore;

  /**
   * Create a session store
   */
  create(vertx: Vertx, sessionMapName: string, reaperInterval: number): LocalSessionStore;
}
