/// <reference path="./throwable.d.ts" />
/// <reference path="./read_stream.d.ts" />

declare module "vertx-js/timeout_stream" {
  export = TimeoutStream;
}

/**
 * A timeout stream is triggered by a timer, the Handler will be call when the timer is fired,
 * it can be once or several times depending on the nature of the timer related to this stream. The
 *  will be called after the timer handler has been called.
 * <p>
 * Pausing the timer inhibits the timer shots until the stream is resumed. Setting a null handler callback cancels
 * the timer.
 */
interface TimeoutStream
  extends
    ReadStream
{
  exceptionHandler(handler: (e: Throwable) => void): TimeoutStream;
  handler(handler: (e: number) => void): TimeoutStream;
  pause(): TimeoutStream;
  resume(): TimeoutStream;
  endHandler(endHandler: (e: void) => void): TimeoutStream;

  /**
   * Cancels the timeout. Note this has the same effect as calling handler with a null
   * argument.
   */
  cancel(): void;
}

declare var TimeoutStream: {
}
