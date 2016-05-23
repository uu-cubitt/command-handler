/// <reference path="./throwable.d.ts" />
/// <reference path="./stream_base.d.ts" />

declare module "vertx-js/write_stream" {
  export = WriteStream;
}

/**
 *
 * Represents a stream of data that can be written to.
 * <p>
 * Any class that implements this interface can be used by a Pump to pump data from a <code>ReadStream</code>
 * to it.
 */
interface WriteStream
  extends
    StreamBase
{

  /**
   * Set an exception handler on the write stream.
   */
  exceptionHandler(handler: (e: Throwable) => void): WriteStream;

  /**
   * Write some data to the stream. The data is put on an internal write queue, and the write actually happens
   * asynchronously. To avoid running out of memory by putting too much on the write queue,
   * check the writeQueueFull method before writing. This is done automatically if using a Pump.
   */
  write(data: any): WriteStream;

  /**
   * Ends the stream.
   * <p>
   * Once the stream has ended, it cannot be used any more.
   */
  end(): void;

  /**
   * Same as end but writes some data to the stream before ending.
   */
  end(t: any): void;

  /**
   * Set the maximum size of the write queue to <code>maxSize</code>. You will still be able to write to the stream even
   * if there is more than <code>maxSize</code> bytes in the write queue. This is used as an indicator by classes such as
   * <code>Pump</code> to provide flow control.
   */
  setWriteQueueMaxSize(maxSize: number): WriteStream;

  /**
   * This will return <code>true</code> if there are more bytes in the write queue than the value set using setWriteQueueMaxSize
   */
  writeQueueFull(): boolean;

  /**
   * Set a drain handler on the stream. If the write queue is full, then the handler will be called when the write
   * queue has been reduced to maxSize / 2. See Pump for an example of this being used.
   */
  drainHandler(handler: (e: void) => void): WriteStream;
}

declare var WriteStream: {
}
