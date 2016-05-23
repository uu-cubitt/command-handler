/// <reference path="./throwable.d.ts" />

declare module "vertx-js/stream_base" {
  export = StreamBase;
}

/**
 * Base interface for a stream.
 */
interface StreamBase
{

  /**
   * Set an exception handler.
   */
  exceptionHandler(handler: (e: Throwable) => void): StreamBase;
}

declare var StreamBase: {
}
