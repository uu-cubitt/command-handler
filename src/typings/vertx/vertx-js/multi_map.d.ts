/// <reference path="./throwable.d.ts" />

declare module "vertx-js/multi_map" {
  export = MultiMap;
}

/**
 * This class represents a MultiMap of String keys to a List of String values.
 * <p>
 * It's useful in Vert.x to represent things in Vert.x like HTTP headers and HTTP parameters which allow
 * multiple values for keys.
 */
interface MultiMap
{

  /**
   * Returns the value of with the specified name.  If there are
   * more than one values for the specified name, the first value is returned.
   */
  get(name: string): string;

  /**
   * Returns the values with the specified name
   */
  getAll(name: string): Array<string>;

  /**
   * Checks to see if there is a value with the specified name
   */
  contains(name: string): boolean;

  /**
   * Return true if empty
   */
  isEmpty(): boolean;

  /**
   * Gets a immutable Set of all names
   */
  names(): Array<string>;

  /**
   * Adds a new value with the specified name and value.
   */
  add(name: string, value: string): MultiMap;

  /**
   * Adds all the entries from another MultiMap to this one
   */
  addAll(map: MultiMap): MultiMap;

  /**
   * Sets a value under the specified name.
   * <p>
   * If there is an existing header with the same name, it is removed.
   */
  set(name: string, value: string): MultiMap;

  /**
   * Cleans this instance.
   */
  setAll(map: MultiMap): MultiMap;

  /**
   * Removes the value with the given name
   */
  remove(name: string): MultiMap;

  /**
   * Removes all
   */
  clear(): MultiMap;

  /**
   * Return the number of keys.
   */
  size(): number;
}

declare var MultiMap: {

  /**
   * Create a multi-map implementation with case insensitive keys, for instance it can be used to hold some HTTP headers.
   */
  caseInsensitiveMultiMap(): MultiMap;
}
