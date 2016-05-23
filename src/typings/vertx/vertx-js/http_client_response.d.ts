/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./multi_map.d.ts" />
/// <reference path="./net_socket.d.ts" />

declare module "vertx-js/http_client_response" {
  export = HttpClientResponse;
}

/**
 * Represents a client-side HTTP response.
 * <p>
 * Vert.x provides you with one of these via the handler that was provided when creating the HttpClientRequest
 * or that was set on the HttpClientRequest instance.
 * <p>
 * It implements ReadStream so it can be used with
 * Pump to pump data with flow control.
 */
interface HttpClientResponse
  extends
    ReadStream
{
  resume(): HttpClientResponse;
  exceptionHandler(handler: (e: Throwable) => void): HttpClientResponse;
  handler(handler: (e: Buffer) => void): HttpClientResponse;
  pause(): HttpClientResponse;
  endHandler(endHandler: (e: void) => void): HttpClientResponse;

  /**
   * @return the status code of the response
   */
  statusCode(): number;

  /**
   * @return the status message of the response
   */
  statusMessage(): string;

  /**
   * @return the headers
   */
  headers(): MultiMap;

  /**
   * Return the first header value with the specified name
   */
  getHeader(headerName: string): string;

  /**
   * Return the first trailer value with the specified name
   */
  getTrailer(trailerName: string): string;

  /**
   * @return the trailers
   */
  trailers(): MultiMap;

  /**
   * @return the Set-Cookie headers (including trailers)
   */
  cookies(): Array<string>;

  /**
   * Convenience method for receiving the entire request body in one piece.
   * <p>
   * This saves you having to manually set a dataHandler and an endHandler and append the chunks of the body until
   * the whole body received. Don't use this if your request body is large - you could potentially run out of RAM.
   */
  bodyHandler(bodyHandler: (e: Buffer) => void): HttpClientResponse;

  /**
   * Get a net socket for the underlying connection of this request.
   * <p>
   * USE THIS WITH CAUTION! Writing to the socket directly if you don't know what you're doing can easily break the HTTP protocol
   * <p>
   * One valid use-case for calling this is to receive the NetSocket after a HTTP CONNECT was issued to the
   * remote peer and it responded with a status code of 200.
   */
  netSocket(): NetSocket;
}

declare var HttpClientResponse: {
}
