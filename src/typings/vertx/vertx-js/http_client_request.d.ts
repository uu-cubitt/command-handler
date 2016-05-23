/// <reference path="./throwable.d.ts" />
/// <reference path="./http_client_response.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./write_stream.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./multi_map.d.ts" />

declare module "vertx-js/http_client_request" {
  export = HttpClientRequest;
}

/**
 * Represents a client-side HTTP request.
 * <p>
 * Instances are created by an HttpClient instance, via one of the methods corresponding to the
 * specific HTTP methods, or the generic request methods. On creation the request will not have been written to the
 * wire.
 * <p>
 * Once a request has been obtained, headers can be set on it, and data can be written to its body if required. Once
 * you are ready to send the request, one of the end methods should be called.
 * <p>
 * Nothing is actually sent until the request has been internally assigned an HTTP connection.
 * <p>
 * The HttpClient instance will return an instance of this class immediately, even if there are no HTTP
 * connections available in the pool. Any requests sent before a connection is assigned will be queued
 * internally and actually sent when an HTTP connection becomes available from the pool.
 * <p>
 * The headers of the request are queued for writing either when the end method is called, or, when the first
 * part of the body is written, whichever occurs first.
 * <p>
 * This class supports both chunked and non-chunked HTTP.
 * <p>
 * It implements WriteStream so it can be used with
 * Pump to pump data with flow control.
 * <p>
 * An example of using this class is as follows:
 * <p>
 */
interface HttpClientRequest
  extends
    WriteStream
  , 
    ReadStream
{

  /**
   * This will return <code>true</code> if there are more bytes in the write queue than the value set using setWriteQueueMaxSize
   */
  writeQueueFull(): boolean;
  exceptionHandler(handler: (e: Throwable) => void): HttpClientRequest;

  /**
   * @throws java.lang.IllegalStateException when no response handler is set
   */
  write(data: Buffer): HttpClientRequest;
  setWriteQueueMaxSize(maxSize: number): HttpClientRequest;
  drainHandler(handler: (e: void) => void): HttpClientRequest;
  handler(handler: (e: HttpClientResponse) => void): HttpClientRequest;
  pause(): HttpClientRequest;
  resume(): HttpClientRequest;
  endHandler(endHandler: (e: void) => void): HttpClientRequest;

  /**
   * If chunked is true then the request will be set into HTTP chunked mode
   */
  setChunked(chunked: boolean): HttpClientRequest;

  /**
   * @return Is the request chunked?
   */
  isChunked(): boolean;

  /**
   * The HTTP method for the request.
   */
  method(): any;

  /**
   * @return The URI of the request.
   */
  uri(): string;

  /**
   * @return The HTTP headers
   */
  headers(): MultiMap;

  /**
   * Put an HTTP header
   */
  putHeader(name: string, value: string): HttpClientRequest;

  /**
   * Write a String to the request body, encoded as UTF-8.
   */
  write(chunk: string): HttpClientRequest;

  /**
   * Write a String to the request body, encoded using the encoding <code>enc</code>.
   */
  write(chunk: string, enc: string): HttpClientRequest;

  /**
   * If you send an HTTP request with the header <code>Expect</code> set to the value <code>100-continue</code>
   * and the server responds with an interim HTTP response with a status code of <code>100</code> and a continue handler
   * has been set using this method, then the <code>handler</code> will be called.
   * <p>
   * You can then continue to write data to the request body and later end it. This is normally used in conjunction with
   * the sendHead method to force the request header to be written before the request has ended.
   */
  continueHandler(handler: (e: void) => void): HttpClientRequest;

  /**
   * Forces the head of the request to be written before end is called on the request or any data is
   * written to it.
   * <p>
   * This is normally used to implement HTTP 100-continue handling, see  for
   * more information.
   */
  sendHead(): HttpClientRequest;

  /**
   * Same as end but writes a String in UTF-8 encoding
   */
  end(chunk: string): void;

  /**
   * Same as end but writes a String with the specified encoding
   */
  end(chunk: string, enc: string): void;

  /**
   * Same as end but writes some data to the request body before ending. If the request is not chunked and
   * no other data has been written then the <code>Content-Length</code> header will be automatically set
   */
  end(chunk: Buffer): void;

  /**
   * Ends the request. If no data has been written to the request body, and sendHead has not been called then
   * the actual request won't get written until this method gets called.
   * <p>
   * Once the request has ended, it cannot be used any more,
   */
  end(): void;

  /**
   * Set's the amount of time after which if the request does not return any data within the timeout period an
   * TimeoutException will be passed to the exception handler (if provided) and
   * the request will be closed.
   * <p>
   * Calling this method more than once has the effect of canceling any existing timeout and starting
   * the timeout from scratch.
   */
  setTimeout(timeoutMs: number): HttpClientRequest;
}

declare var HttpClientRequest: {
}
