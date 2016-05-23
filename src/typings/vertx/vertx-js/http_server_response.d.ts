/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./write_stream.d.ts" />
/// <reference path="./multi_map.d.ts" />

declare module "vertx-js/http_server_response" {
  export = HttpServerResponse;
}

/**
 * Represents a server-side HTTP response.
 * <p>
 * An instance of this is created and associated to every instance of
 * HttpServerRequest that.
 * <p>
 * It allows the developer to control the HTTP response that is sent back to the
 * client for a particular HTTP request.
 * <p>
 * It contains methods that allow HTTP headers and trailers to be set, and for a body to be written out to the response.
 * <p>
 * It also allows files to be streamed by the kernel directly from disk to the
 * outgoing HTTP connection, bypassing user space altogether (where supported by
 * the underlying operating system). This is a very efficient way of
 * serving files from the server since buffers do not have to be read one by one
 * from the file and written to the outgoing socket.
 * <p>
 * It implements WriteStream so it can be used with
 * Pump to pump data with flow control.
 */
interface HttpServerResponse
  extends
    WriteStream
{

  /**
   * This will return <code>true</code> if there are more bytes in the write queue than the value set using setWriteQueueMaxSize
   */
  writeQueueFull(): boolean;
  exceptionHandler(handler: (e: Throwable) => void): HttpServerResponse;
  write(data: Buffer): HttpServerResponse;
  setWriteQueueMaxSize(maxSize: number): HttpServerResponse;
  drainHandler(handler: (e: void) => void): HttpServerResponse;

  /**
   * @return the HTTP status code of the response. The default is <code>200</code> representing <code>OK</code>.
   */
  getStatusCode(): number;

  /**
   * Set the status code. If the status message hasn't been explicitly set, a default status message corresponding
   * to the code will be looked-up and used.
   */
  setStatusCode(statusCode: number): HttpServerResponse;

  /**
   * @return the HTTP status message of the response. If this is not specified a default value will be used depending on what
   * setStatusCode has been set to.
   */
  getStatusMessage(): string;

  /**
   * Set the status message
   */
  setStatusMessage(statusMessage: string): HttpServerResponse;

  /**
   * If <code>chunked</code> is <code>true</code>, this response will use HTTP chunked encoding, and each call to write to the body
   * will correspond to a new HTTP chunk sent on the wire.
   * <p>
   * If chunked encoding is used the HTTP header <code>Transfer-Encoding</code> with a value of <code>Chunked</code> will be
   * automatically inserted in the response.
   * <p>
   * If <code>chunked</code> is <code>false</code>, this response will not use HTTP chunked encoding, and therefore the total size
   * of any data that is written in the respone body must be set in the <code>Content-Length</code> header <b>before</b> any
   * data is written out.
   * <p>
   * An HTTP chunked response is typically used when you do not know the total size of the request body up front.
   */
  setChunked(chunked: boolean): HttpServerResponse;

  /**
   * @return is the response chunked?
   */
  isChunked(): boolean;

  /**
   * @return The HTTP headers
   */
  headers(): MultiMap;

  /**
   * Put an HTTP header
   */
  putHeader(name: string, value: string): HttpServerResponse;

  /**
   * @return The HTTP trailers
   */
  trailers(): MultiMap;

  /**
   * Put an HTTP trailer
   */
  putTrailer(name: string, value: string): HttpServerResponse;

  /**
   * Set a close handler for the response. This will be called if the underlying connection closes before the response
   * is complete.
   */
  closeHandler(handler: (e: void) => void): HttpServerResponse;

  /**
   * Write a String to the response body, encoded using the encoding <code>enc</code>.
   */
  write(chunk: string, enc: string): HttpServerResponse;

  /**
   * Write a String to the response body, encoded in UTF-8.
   */
  write(chunk: string): HttpServerResponse;

  /**
   * Used to write an interim 100 Continue response to signify that the client should send the rest of the request.
   * Must only be used if the request contains an "Expect:100-Continue" header
   */
  writeContinue(): HttpServerResponse;

  /**
   * Same as end but writes a String in UTF-8 encoding before ending the response.
   */
  end(chunk: string): void;

  /**
   * Same as end but writes a String with the specified encoding before ending the response.
   */
  end(chunk: string, enc: string): void;

  /**
   * Same as end but writes some data to the response body before ending. If the response is not chunked and
   * no other data has been written then the @code{Content-Length} header will be automatically set.
   */
  end(chunk: Buffer): void;

  /**
   * Ends the response. If no data has been written to the response body,
   * the actual response won't get written until this method gets called.
   * <p>
   * Once the response has ended, it cannot be used any more.
   */
  end(): void;

  /**
   * Same as sendFile using offset @code{0} which means starting from the beginning of the file.
   */
  sendFile(filename: string): HttpServerResponse;

  /**
   * Same as sendFile using length @code{Long.MAX_VALUE} which means until the end of the
   * file.
   */
  sendFile(filename: string, offset: number): HttpServerResponse;

  /**
   * Ask the OS to stream a file as specified by <code>filename</code> directly
   * from disk to the outgoing connection, bypassing userspace altogether
   * (where supported by the underlying operating system.
   * This is a very efficient way to serve files.<p>
   * The actual serve is asynchronous and may not complete until some time after this method has returned.
   */
  sendFile(filename: string, offset: number, length: number): HttpServerResponse;

  /**
   * Like sendFile but providing a handler which will be notified once the file has been completely
   * written to the wire.
   */
  sendFile(filename: string, resultHandler: (res: void, err?: Throwable) => void): HttpServerResponse;

  /**
   * Like sendFile but providing a handler which will be notified once the file has been completely
   * written to the wire.
   */
  sendFile(filename: string, offset: number, resultHandler: (res: void, err?: Throwable) => void): HttpServerResponse;

  /**
   * Like sendFile but providing a handler which will be notified once the file has been
   * completely written to the wire.
   */
  sendFile(filename: string, offset: number, length: number, resultHandler: (res: void, err?: Throwable) => void): HttpServerResponse;

  /**
   * Close the underlying TCP connection corresponding to the request.
   */
  close(): void;

  /**
   * @return has the response already ended?
   */
  ended(): boolean;

  /**
   * @return has the underlying TCP connection corresponding to the request already been closed?
   */
  closed(): boolean;

  /**
   * @return have the headers for the response already been written?
   */
  headWritten(): boolean;

  /**
   * Provide a handler that will be called just before the headers are written to the wire.<p>
   * This provides a hook allowing you to add any more headers or do any more operations before this occurs.
   */
  headersEndHandler(handler: (e: void) => void): HttpServerResponse;

  /**
   * Provide a handler that will be called just before the last part of the body is written to the wire
   * and the response is ended.<p>
   * This provides a hook allowing you to do any more operations before this occurs.
   */
  bodyEndHandler(handler: (e: void) => void): HttpServerResponse;

  /**
   * @return the total number of bytes written for the body of the response.
   */
  bytesWritten(): number;
}

declare var HttpServerResponse: {
}
