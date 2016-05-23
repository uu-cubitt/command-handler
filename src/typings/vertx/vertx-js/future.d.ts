/// <reference path="./throwable.d.ts" />

declare module "vertx-js/future" {
  export = Future;
}

/**
 * Represents the result of an action that may, or may not, have occurred yet.
 * <p>
 */
interface Future
{

  /**
   * Has the future completed?
   * <p>
   * It's completed if it's either succeeded or failed.
   */
  isComplete(): boolean;

  /**
   * Set a handler for the result.
   * <p>
   * If the future has already been completed it will be called immediately. Otherwise it will be called when the
   * future is completed.
   */
  setHandler(handler: (res: any, err?: Throwable) => void): void;

  /**
   * Set the result. Any handler will be called, if there is one, and the future will be marked as completed.
   */
  complete(result: any): void;

  /**
   * Set a null result. Any handler will be called, if there is one, and the future will be marked as completed.
   */
  complete(): void;

  /**
   * Set the failure. Any handler will be called, if there is one, and the future will be marked as completed.
   */
  fail(failureMessage: string): void;
}

declare var Future: {

  /**
   * Create a future that hasn't completed yet
   */
  future(): Future;

  /**
   * Create a succeeded future with a null result
   */
  succeededFuture(): Future;

  /**
   * Created a succeeded future with the specified result.
   */
  succeededFuture(result: any): Future;

  /**
   * Create a failed future with the specified failure message.
   */
  failedFuture(failureMessage: string): Future;
}
