/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./write_stream.d.ts" />
/// <reference path="./read_stream.d.ts" />

declare module "vertx-js/async_file" {
  export = AsyncFile;
}

/**
 * Represents a file on the file-system which can be read from, or written to asynchronously.
 * <p>
 * This class also implements ReadStream and
 * WriteStream. This allows the data to be pumped to and from
 * other streams, e.g. an HttpClientRequest instance,
 * using the Pump class
 */
interface AsyncFile
  extends
    ReadStream
  , 
    WriteStream
{

  /**
   * Same as end but writes some data to the stream before ending.
   */
  end(t: Buffer): void;

  /**
   * This will return <code>true</code> if there are more bytes in the write queue than the value set using setWriteQueueMaxSize
   */
  writeQueueFull(): boolean;
  handler(handler: (e: Buffer) => void): AsyncFile;
  pause(): AsyncFile;
  resume(): AsyncFile;
  endHandler(endHandler: (e: void) => void): AsyncFile;
  write(data: Buffer): AsyncFile;
  setWriteQueueMaxSize(maxSize: number): AsyncFile;
  drainHandler(handler: (e: void) => void): AsyncFile;
  exceptionHandler(handler: (e: Throwable) => void): AsyncFile;

  /**
   * Close the file, see close.
   */
  end(): void;

  /**
   * Close the file. The actual close happens asynchronously.
   */
  close(): void;

  /**
   * Close the file. The actual close happens asynchronously.
   * The handler will be called when the close is complete, or an error occurs.
   */
  close(handler: (res: void, err?: Throwable) => void): void;

  /**
   * Write a Buffer to the file at position <code>position</code> in the file, asynchronously.
   * <p>
   * If <code>position</code> lies outside of the current size
   * of the file, the file will be enlarged to encompass it.
   * <p>
   * When multiple writes are invoked on the same file
   * there are no guarantees as to order in which those writes actually occur
   * <p>
   * The handler will be called when the write is complete, or if an error occurs.
   */
  write(buffer: Buffer, position: number, handler: (res: void, err?: Throwable) => void): AsyncFile;

  /**
   * Reads <code>length</code> bytes of data from the file at position <code>position</code> in the file, asynchronously.
   * <p>
   * The read data will be written into the specified <code>Buffer buffer</code> at position <code>offset</code>.
   * <p>
   * If data is read past the end of the file then zero bytes will be read.<p>
   * When multiple reads are invoked on the same file there are no guarantees as to order in which those reads actually occur.
   * <p>
   * The handler will be called when the close is complete, or if an error occurs.
   */
  read(buffer: Buffer, offset: number, position: number, length: number, handler: (res: Buffer, err?: Throwable) => void): AsyncFile;

  /**
   * Flush any writes made to this file to underlying persistent storage.
   * <p>
   * If the file was opened with <code>flush</code> set to <code>true</code> then calling this method will have no effect.
   * <p>
   * The actual flush will happen asynchronously.
   */
  flush(): AsyncFile;

  /**
   * Same as flush but the handler will be called when the flush is complete or if an error occurs
   */
  flush(handler: (res: void, err?: Throwable) => void): AsyncFile;

  /**
   * Sets the position from which data will be read from when using the file as a ReadStream.
   */
  setReadPos(readPos: number): AsyncFile;

  /**
   * Sets the position from which data will be written when using the file as a WriteStream.
   */
  setWritePos(writePos: number): AsyncFile;

  /**
   * Sets the buffer size that will be used to read the data from the file. Changing this value will impact how much
   * the data will be read at a time from the file system.
   */
  setReadBufferSize(readBufferSize: number): AsyncFile;
}

declare var AsyncFile: {
}
