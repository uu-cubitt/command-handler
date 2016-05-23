/// <reference path="./throwable.d.ts" />

declare module "vertx-js/local_map" {
  export = LocalMap;
}

/**
 * Local maps can be used to share data safely in a single Vert.x instance.
 * <p>
 * The map only allows immutable keys and values in the map, OR certain mutable objects such as Buffer
 * instances which will be copied when they are added to the map.
 * <p>
 * This ensures there is no shared access to mutable state from different threads (e.g. different event loops) in the
 * Vert.x instance, and means you don't have to protect access to that state using synchronization or locks.
 */
interface LocalMap
{

  /**
   * Get a value from the map
   */
  get(key: any): any;

  /**
   * Put an entry in the map
   */
  put(key: any, value: any): any;

  /**
   * Remove an entry from the map
   */
  remove(key: any): any;

  /**
   * Clear all entries in the map
   */
  clear(): void;

  /**
   * Get the size of the map
   */
  size(): number;

  /**
   * @return true if there are zero entries in the map
   */
  isEmpty(): boolean;

  /**
   * Put the entry only if there is no existing entry for that key
   */
  putIfAbsent(key: any, value: any): any;

  /**
   * Remove the entry only if there is an entry with the specified key and value
   */
  removeIfPresent(key: any, value: any): boolean;

  /**
   * Replace the entry only if there is an existing entry with the specified key and value
   */
  replaceIfPresent(key: any, oldValue: any, newValue: any): boolean;

  /**
   * Replace the entry only if there is an existing entry with the key
   */
  replace(key: any, value: any): any;

  /**
   * Close and release the map
   */
  close(): void;
}

declare var LocalMap: {
}
