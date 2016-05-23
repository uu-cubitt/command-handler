/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/static_handler" {
  export = StaticHandler;
}

/**
 * A handler for serving static resources from the file system or classpath.
 */
interface StaticHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;

  /**
   * Enable/Disable access to the root of the filesystem
   */
  setAllowRootFileSystemAccess(allowRootFileSystemAccess: boolean): StaticHandler;

  /**
   * Set the web root
   */
  setWebRoot(webRoot: string): StaticHandler;

  /**
   * Set whether files are read-only and will never change
   */
  setFilesReadOnly(readOnly: boolean): StaticHandler;

  /**
   * Set value for max age in caching headers
   */
  setMaxAgeSeconds(maxAgeSeconds: number): StaticHandler;

  /**
   * Set whether cache header handling is enabled
   */
  setCachingEnabled(enabled: boolean): StaticHandler;

  /**
   * Set whether directory listing is enabled
   */
  setDirectoryListing(directoryListing: boolean): StaticHandler;

  /**
   * Set whether hidden files should be served
   */
  setIncludeHidden(includeHidden: boolean): StaticHandler;

  /**
   * Set the server cache entry timeout when caching is enabled
   */
  setCacheEntryTimeout(timeout: number): StaticHandler;

  /**
   * Set the index page
   */
  setIndexPage(indexPage: string): StaticHandler;

  /**
   * Set the max cache size, when caching is enabled
   */
  setMaxCacheSize(maxCacheSize: number): StaticHandler;

  /**
   * Set whether async filesystem access should always be used
   */
  setAlwaysAsyncFS(alwaysAsyncFS: boolean): StaticHandler;

  /**
   * Set whether async/sync filesystem tuning should enabled
   */
  setEnableFSTuning(enableFSTuning: boolean): StaticHandler;

  /**
   * Set the max serve time in ns, above which serves are considered slow
   */
  setMaxAvgServeTimeNs(maxAvgServeTimeNanoSeconds: number): StaticHandler;

  /**
   * Set the directory template to be used when directory listing
   */
  setDirectoryTemplate(directoryTemplate: string): StaticHandler;

  /**
   * Set whether range requests (resumable downloads; media streaming) should be enabled.
   */
  setEnableRangeSupport(enableRangeSupport: boolean): StaticHandler;
}

declare var StaticHandler: {

  /**
   * Create a handler using defaults
   */
  create(): StaticHandler;

  /**
   * Create a handler, specifying web-root
   */
  create(root: string): StaticHandler;
}
