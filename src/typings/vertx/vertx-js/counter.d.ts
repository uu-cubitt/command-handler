/// <reference path="./throwable.d.ts" />

declare module "vertx-js/counter" {
  export = Counter;
}

/**
 * An asynchronous counter that can be used to across the cluster to maintain a consistent count.
 * <p>
 */
interface Counter
{

  /**
   * Get the current value of the counter
   */
  get(resultHandler: (res: number, err?: Throwable) => void): void;

  /**
   * Increment the counter atomically and return the new count
   */
  incrementAndGet(resultHandler: (res: number, err?: Throwable) => void): void;

  /**
   * Increment the counter atomically and return the value before the increment.
   */
  getAndIncrement(resultHandler: (res: number, err?: Throwable) => void): void;

  /**
   * Decrement the counter atomically and return the new count
   */
  decrementAndGet(resultHandler: (res: number, err?: Throwable) => void): void;

  /**
   * Add the value to the counter atomically and return the new count
   */
  addAndGet(value: number, resultHandler: (res: number, err?: Throwable) => void): void;

  /**
   * Add the value to the counter atomically and return the value before the add
   */
  getAndAdd(value: number, resultHandler: (res: number, err?: Throwable) => void): void;

  /**
   * Set the counter to the specified value only if the current value is the expectec value. This happens
   * atomically.
   */
  compareAndSet(expected: number, value: number, resultHandler: (res: boolean, err?: Throwable) => void): void;
}

declare var Counter: {
}
