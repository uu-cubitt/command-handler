/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./write_stream.d.ts" />

declare module "vertx-js/packet_writestream" {
  export = PacketWritestream;
}

/**
 * A WriteStream for sending packets to a SocketAddress.
 * The stream  is called when the write fails.
 */
interface PacketWritestream
  extends
    WriteStream
{

  /**
   * Ends the stream.
   * <p>
   * Once the stream has ended, it cannot be used any more.
   */
  end(): void;

  /**
   * Same as end but writes some data to the stream before ending.
   */
  end(t: Buffer): void;

  /**
   * This will return <code>true</code> if there are more bytes in the write queue than the value set using setWriteQueueMaxSize
   */
  writeQueueFull(): boolean;
  exceptionHandler(handler: (e: Throwable) => void): PacketWritestream;
  write(data: Buffer): PacketWritestream;
  setWriteQueueMaxSize(maxSize: number): PacketWritestream;
  drainHandler(handler: (e: void) => void): PacketWritestream;
}

declare var PacketWritestream: {
}
