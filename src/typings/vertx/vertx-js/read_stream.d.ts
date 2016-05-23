/// <reference path="./throwable.d.ts" />
/// <reference path="./stream_base.d.ts" />

declare module "vertx-js/read_stream" {
  export = ReadStream;
}

/**
 * Represents a stream of items that can be read from.
 * <p>
 * Any class that implements this interface can be used by a Pump to pump data from it
 * to a WriteStream.
 */
interface ReadStream
  extends
    StreamBase
{

  /**
   * Set an exception handler on the read stream.
   */
  exceptionHandler(handler: (e: Throwable) => void): ReadStream;

  /**
   * Set a data handler. As data is read, the handler will be called with the data.
   */
  handler(handler: (e: any) => void): ReadStream;

  /**
   * Pause the <code>ReadSupport</code>. While it's paused, no data will be sent to the <code>dataHandler</code>
   */
  pause(): ReadStream;

  /**
   * Resume reading. If the <code>ReadSupport</code> has been paused, reading will recommence on it.
   */
  resume(): ReadStream;

  /**
   * Set an end handler. Once the stream has ended, and there is no more data to be read, this handler will be called.
   */
  endHandler(endHandler: (e: void) => void): ReadStream;
}

declare var ReadStream: {
}
