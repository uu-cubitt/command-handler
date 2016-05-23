/// <reference path="./throwable.d.ts" />

declare module "vertx-js/async_map" {
  export = AsyncMap;
}

/**
 *
 * An asynchronous map.
 */
interface AsyncMap
{

  /**
   * Get a value from the map, asynchronously.
   */
  get(k: any, resultHandler: (res: any, err?: Throwable) => void): void;

  /**
   * Put a value in the map, asynchronously.
   */
  put(k: any, v: any, completionHandler: (res: void, err?: Throwable) => void): void;

  /**
   * Like put but specifying a timeout. If the value cannot be put within the timeout a
   * failure will be passed to the handler
   */
  put(k: any, v: any, ttl: number, completionHandler: (res: void, err?: Throwable) => void): void;

  /**
   * Put the entry only if there is no entry with the key already present. If key already present then the existing
   * value will be returned to the handler, otherwise null.
   */
  putIfAbsent(k: any, v: any, completionHandler: (res: any, err?: Throwable) => void): void;

  /**
   * Link putIfAbsent but specifying a timeout. If the value cannot be put within the timeout a
   * failure will be passed to the handler
   */
  putIfAbsent(k: any, v: any, ttl: number, completionHandler: (res: any, err?: Throwable) => void): void;

  /**
   * Remove a value from the map, asynchronously.
   */
  remove(k: any, resultHandler: (res: any, err?: Throwable) => void): void;

  /**
   * Remove a value from the map, only if entry already exists with same value.
   */
  removeIfPresent(k: any, v: any, resultHandler: (res: boolean, err?: Throwable) => void): void;

  /**
   * Replace the entry only if it is currently mapped to some value
   */
  replace(k: any, v: any, resultHandler: (res: any, err?: Throwable) => void): void;

  /**
   * Replace the entry only if it is currently mapped to a specific value
   */
  replaceIfPresent(k: any, oldValue: any, newValue: any, resultHandler: (res: boolean, err?: Throwable) => void): void;

  /**
   * Clear all entries in the map
   */
  clear(resultHandler: (res: void, err?: Throwable) => void): void;

  /**
   * Provide the number of entries in the map
   */
  size(resultHandler: (res: number, err?: Throwable) => void): void;
}

declare var AsyncMap: {
}
