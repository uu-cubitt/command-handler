/// <reference path="./throwable.d.ts" />
/// <reference path="./http_client_request.d.ts" />
/// <reference path="./http_client_response.d.ts" />
/// <reference path="./measured.d.ts" />
/// <reference path="./web_socket_stream.d.ts" />
/// <reference path="./multi_map.d.ts" />
/// <reference path="./web_socket.d.ts" />

declare module "vertx-js/http_client" {
  export = HttpClient;
}

/**
 * An asynchronous HTTP client.
 * <p>
 * It allows you to make requests to HTTP servers, and a single client can make requests to any server.
 * <p>
 * It also allows you to open WebSockets to servers.
 * <p>
 * The client can also pool HTTP connections.
 * <p>
 * For pooling to occur, keep-alive must be true on the HttpClientOptions (default is true).
 * In this case connections will be pooled and re-used if there are pending HTTP requests waiting to get a connection,
 * otherwise they will be closed.
 * <p>
 * This gives the benefits of keep alive when the client is loaded but means we don't keep connections hanging around
 * unnecessarily when there would be no benefits anyway.
 * <p>
 * The client also supports pipe-lining of requests. Pipe-lining means another request is sent on the same connection
 * before the response from the preceeding one has returned. Pipe-lining is not appropriate for all requests.
 * <p>
 * To enable pipe-lining, it must be enabled on the HttpClientOptions (default is false).
 * <p>
 * When pipe-lining is enabled the connection will be automatically closed when all in-flight responses have returned
 * and there are no outstanding pending requests to write.
 * <p>
 * The client is designed to be reused between requests.
 */
interface HttpClient
  extends
    Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;

  /**
   * Create an HTTP request to send to the server at the specified host and port.
   */
  request(method: any, port: number, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP request to send to the server at the specified host and default port.
   */
  request(method: any, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP request to send to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  request(method: any, port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP request to send to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  request(method: any, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP request to send to the server at the default host and port.
   */
  request(method: any, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP request to send to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  request(method: any, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP request to send to the server using an absolute URI
   */
  requestAbs(method: any, absoluteURI: string): HttpClientRequest;

  /**
   * Create an HTTP request to send to the server using an absolute URI, specifying a response handler to receive
   * the response
   */
  requestAbs(method: any, absoluteURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server at the specified host and port.
   */
  get(port: number, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server at the specified host and default port.
   */
  get(host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  get(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  get(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server at the default host and port.
   */
  get(requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  get(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server using an absolute URI
   */
  getAbs(absoluteURI: string): HttpClientRequest;

  /**
   * Create an HTTP GET request to send to the server using an absolute URI, specifying a response handler to receive
   * the response
   */
  getAbs(absoluteURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Sends an HTTP GET request to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  getNow(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Sends an HTTP GET request to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  getNow(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Sends an HTTP GET request  to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  getNow(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Create an HTTP POST request to send to the server at the specified host and port.
   */
  post(port: number, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP POST request to send to the server at the specified host and default port.
   */
  post(host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP POST request to send to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  post(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP POST request to send to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  post(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP POST request to send to the server at the default host and port.
   */
  post(requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP POST request to send to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  post(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP POST request to send to the server using an absolute URI
   */
  postAbs(absoluteURI: string): HttpClientRequest;

  /**
   * Create an HTTP POST request to send to the server using an absolute URI, specifying a response handler to receive
   * the response
   */
  postAbs(absoluteURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server at the specified host and port.
   */
  head(port: number, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server at the specified host and default port.
   */
  head(host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  head(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  head(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server at the default host and port.
   */
  head(requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  head(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server using an absolute URI
   */
  headAbs(absoluteURI: string): HttpClientRequest;

  /**
   * Create an HTTP HEAD request to send to the server using an absolute URI, specifying a response handler to receive
   * the response
   */
  headAbs(absoluteURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Sends an HTTP HEAD request to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  headNow(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Sends an HTTP HEAD request to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  headNow(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Sends an HTTP HEAD request  to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  headNow(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Create an HTTP OPTIONS request to send to the server at the specified host and port.
   */
  options(port: number, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP OPTIONS request to send to the server at the specified host and default port.
   */
  options(host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP OPTIONS request to send to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  options(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP OPTIONS request to send to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  options(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP OPTIONS request to send to the server at the default host and port.
   */
  options(requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP OPTIONS request to send to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  options(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP OPTIONS request to send to the server using an absolute URI
   */
  optionsAbs(absoluteURI: string): HttpClientRequest;

  /**
   * Create an HTTP OPTIONS request to send to the server using an absolute URI, specifying a response handler to receive
   * the response
   */
  optionsAbs(absoluteURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Sends an HTTP OPTIONS request to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  optionsNow(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Sends an HTTP OPTIONS request to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  optionsNow(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Sends an HTTP OPTIONS request  to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  optionsNow(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClient;

  /**
   * Create an HTTP PUT request to send to the server at the specified host and port.
   */
  put(port: number, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP PUT request to send to the server at the specified host and default port.
   */
  put(host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP PUT request to send to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  put(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP PUT request to send to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  put(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP PUT request to send to the server at the default host and port.
   */
  put(requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP PUT request to send to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  put(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP PUT request to send to the server using an absolute URI
   */
  putAbs(absoluteURI: string): HttpClientRequest;

  /**
   * Create an HTTP PUT request to send to the server using an absolute URI, specifying a response handler to receive
   * the response
   */
  putAbs(absoluteURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server at the specified host and port.
   */
  delete(port: number, host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server at the specified host and default port.
   */
  delete(host: string, requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server at the specified host and port, specifying a response handler to receive
   * the response
   */
  delete(port: number, host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server at the specified host and default port, specifying a response handler to receive
   * the response
   */
  delete(host: string, requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server at the default host and port.
   */
  delete(requestURI: string): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server at the default host and port, specifying a response handler to receive
   * the response
   */
  delete(requestURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server using an absolute URI
   */
  deleteAbs(absoluteURI: string): HttpClientRequest;

  /**
   * Create an HTTP DELETE request to send to the server using an absolute URI, specifying a response handler to receive
   * the response
   */
  deleteAbs(absoluteURI: string, responseHandler: (e: HttpClientResponse) => void): HttpClientRequest;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI
   */
  websocket(port: number, host: string, requestURI: string, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI
   */
  websocket(port: number, host: string, requestURI: string, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket to the host and relative request URI and default port
   */
  websocket(host: string, requestURI: string, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the host and relative request URI and default port
   */
  websocket(host: string, requestURI: string, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI, and with the specified headers
   */
  websocket(port: number, host: string, requestURI: string, headers: MultiMap, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI, and with the specified headers
   */
  websocket(port: number, host: string, requestURI: string, headers: MultiMap, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified host,relative request UR, and default port and with the specified headers
   */
  websocket(host: string, requestURI: string, headers: MultiMap, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified host,relative request UR, and default port and with the specified headers
   */
  websocket(host: string, requestURI: string, headers: MultiMap, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI, with the specified headers and using
   * the specified version of WebSockets
   */
  websocket(port: number, host: string, requestURI: string, headers: MultiMap, version: any, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI, with the specified headers and using
   * the specified version of WebSockets
   */
  websocket(port: number, host: string, requestURI: string, headers: MultiMap, version: any, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified host, relative request URI and default port with the specified headers and using
   * the specified version of WebSockets
   */
  websocket(host: string, requestURI: string, headers: MultiMap, version: any, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified host, relative request URI and default port with the specified headers and using
   * the specified version of WebSockets
   */
  websocket(host: string, requestURI: string, headers: MultiMap, version: any, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI, with the specified headers, using
   * the specified version of WebSockets, and the specified websocket sub protocols
   */
  websocket(port: number, host: string, requestURI: string, headers: MultiMap, version: any, subProtocols: string, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified port, host and relative request URI, with the specified headers, using
   * the specified version of WebSockets, and the specified websocket sub protocols
   */
  websocket(port: number, host: string, requestURI: string, headers: MultiMap, version: any, subProtocols: string, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified host, relative request URI and default port, with the specified headers, using
   * the specified version of WebSockets, and the specified websocket sub protocols
   */
  websocket(host: string, requestURI: string, headers: MultiMap, version: any, subProtocols: string, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket to the specified host, relative request URI and default port, with the specified headers, using
   * the specified version of WebSockets, and the specified websocket sub protocols
   */
  websocket(host: string, requestURI: string, headers: MultiMap, version: any, subProtocols: string, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port
   */
  websocket(requestURI: string, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port
   */
  websocket(requestURI: string, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port and the specified headers
   */
  websocket(requestURI: string, headers: MultiMap, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port and the specified headers
   */
  websocket(requestURI: string, headers: MultiMap, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port, the specified headers and the
   * specified version of WebSockets
   */
  websocket(requestURI: string, headers: MultiMap, version: any, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port, the specified headers and the
   * specified version of WebSockets
   */
  websocket(requestURI: string, headers: MultiMap, version: any, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port, the specified headers, the
   * specified version of WebSockets and the specified sub protocols
   */
  websocket(requestURI: string, headers: MultiMap, version: any, subProtocols: string, wsConnect: (e: WebSocket) => void): HttpClient;

  /**
   * Connect a WebSocket at the relative request URI using the default host and port, the specified headers, the
   * specified version of WebSockets and the specified sub protocols
   */
  websocket(requestURI: string, headers: MultiMap, version: any, subProtocols: string, wsConnect: (e: WebSocket) => void, failureHandler: (e: Throwable) => void): HttpClient;

  /**
   * Create a WebSocket stream to the specified port, host and relative request URI
   */
  websocketStream(port: number, host: string, requestURI: string): WebSocketStream;

  /**
   * Create a WebSocket stream to the specified host, relative request URI and default port
   */
  websocketStream(host: string, requestURI: string): WebSocketStream;

  /**
   * Create a WebSocket stream to the specified port, host and relative request URI, and with the specified headers
   */
  websocketStream(port: number, host: string, requestURI: string, headers: MultiMap): WebSocketStream;

  /**
   * Create a WebSocket stream to the specified host, relative request URI and default port and with the specified headers
   */
  websocketStream(host: string, requestURI: string, headers: MultiMap): WebSocketStream;

  /**
   * Create a WebSocket stream to the specified port, host and relative request URI, with the specified headers and using
   * the specified version of WebSockets
   */
  websocketStream(port: number, host: string, requestURI: string, headers: MultiMap, version: any): WebSocketStream;

  /**
   * Create a WebSocket stream to the specified host, relative request URI and default port and with the specified headers and using
   * the specified version of WebSockets
   */
  websocketStream(host: string, requestURI: string, headers: MultiMap, version: any): WebSocketStream;

  /**
   * Create a WebSocket stream to the specified port, host and relative request URI, with the specified headers, using
   * the specified version of WebSockets, and the specified websocket sub protocols
   */
  websocketStream(port: number, host: string, requestURI: string, headers: MultiMap, version: any, subProtocols: string): WebSocketStream;

  /**
   * Create a WebSocket stream to the specified host, relative request URI and default port, with the specified headers, using
   * the specified version of WebSockets, and the specified websocket sub protocols
   */
  websocketStream(host: string, requestURI: string, headers: MultiMap, version: any, subProtocols: string): WebSocketStream;

  /**
   * Create a WebSocket stream at the relative request URI using the default host and port and the specified headers
   */
  websocketStream(requestURI: string): WebSocketStream;

  /**
   * Create a WebSocket stream at the relative request URI using the default host and port and the specified headers
   */
  websocketStream(requestURI: string, headers: MultiMap): WebSocketStream;

  /**
   * Create a WebSocket stream at the relative request URI using the default host and port, the specified headers and the
   * specified version of WebSockets
   */
  websocketStream(requestURI: string, headers: MultiMap, version: any): WebSocketStream;

  /**
   * Create a WebSocket stream at the relative request URI using the default host and port, the specified headers, the
   * specified version of WebSockets and the specified sub protocols
   */
  websocketStream(requestURI: string, headers: MultiMap, version: any, subProtocols: string): WebSocketStream;

  /**
   * Close the client. Closing will close down any pooled connections.
   * Clients should always be closed after use.
   */
  close(): void;
}

declare var HttpClient: {
}
