/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/body_handler" {
  export = BodyHandler;
}

/**
 * A handler which gathers the entire request body and sets it on the .
 * <p>
 * It also handles HTTP file uploads and can be used to limit body sizes.
 */
interface BodyHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;

  /**
   * Set the maximum body size -1 means unlimited
   */
  setBodyLimit(bodyLimit: number): BodyHandler;

  /**
   * Set the uploads directory to use
   */
  setUploadsDirectory(uploadsDirectory: string): BodyHandler;

  /**
   * Set whether form attributes will be added to the request parameters
   */
  setMergeFormAttributes(mergeFormAttributes: boolean): BodyHandler;
}

declare var BodyHandler: {

  /**
   * Create a body handler with defaults
   */
  create(): BodyHandler;

  /**
   * Create a body handler and use the given upload directory.
   */
  create(uploadDirectory: string): BodyHandler;
}
