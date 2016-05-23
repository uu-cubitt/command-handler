/// <reference path="./throwable.d.ts" />

declare module "vertx-js/lock" {
  export = Lock;
}

/**
 * An asynchronous exclusive lock which can be obtained from any node in the cluster.
 * <p>
 * When the lock is obtained, no-one else in the cluster can obtain the lock with the same name until the lock
 * is released.
 */
interface Lock
{

  /**
   * Release the lock. Once the lock is released another will be able to obtain the lock.
   */
  release(): void;
}

declare var Lock: {
}
