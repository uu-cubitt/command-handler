/// <reference path="./throwable.d.ts" />
/// <reference path="./http_server_request.d.ts" />
/// <reference path="./server_web_socket.d.ts" />
/// <reference path="./measured.d.ts" />
/// <reference path="./http_server_request_stream.d.ts" />
/// <reference path="./server_web_socket_stream.d.ts" />

declare module "vertx-js/http_server" {
  export = HttpServer;
}

/**
 * An HTTP and WebSockets server.
 * <p>
 * You receive HTTP requests by providing a requestHandler. As requests arrive on the server the handler
 * will be called with the requests.
 * <p>
 * You receive WebSockets by providing a websocketHandler. As WebSocket connections arrive on the server, the
 * WebSocket is passed to the handler.
 */
interface HttpServer
  extends
    Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;

  /**
   * Return the request stream for the server. As HTTP requests are received by the server,
   * instances of HttpServerRequest will be created and passed to the stream .
   */
  requestStream(): HttpServerRequestStream;

  /**
   * Set the request handler for the server to <code>requestHandler</code>. As HTTP requests are received by the server,
   * instances of HttpServerRequest will be created and passed to this handler.
   */
  requestHandler(handler: (e: HttpServerRequest) => void): HttpServer;

  /**
   * Return the websocket stream for the server. If a websocket connect handshake is successful a
   * new ServerWebSocket instance will be created and passed to the stream .
   */
  websocketStream(): ServerWebSocketStream;

  /**
   * Set the websocket handler for the server to <code>wsHandler</code>. If a websocket connect handshake is successful a
   * new ServerWebSocket instance will be created and passed to the handler.
   */
  websocketHandler(handler: (e: ServerWebSocket) => void): HttpServer;

  /**
   * Tell the server to start listening. The server will listen on the port and host specified in the
   * HttpServerOptions that was used when creating the server.
   * <p>
   * The listen happens asynchronously and the server may not be listening until some time after the call has returned.
   */
  listen(): HttpServer;

  /**
   * Tell the server to start listening. The server will listen on the port and host specified here,
   * ignoring any value set in the HttpServerOptions that was used when creating the server.
   * <p>
   * The listen happens asynchronously and the server may not be listening until some time after the call has returned.
   */
  listen(port: number, host: string): HttpServer;

  /**
   * Like listen but supplying a handler that will be called when the server is actually
   * listening (or has failed).
   */
  listen(port: number, host: string, listenHandler: (res: HttpServer, err?: Throwable) => void): HttpServer;

  /**
   * Like listen but the server will listen on host "0.0.0.0" and port specified here ignoring
   * any value in the HttpServerOptions that was used when creating the server.
   */
  listen(port: number): HttpServer;

  /**
   * Like listen but supplying a handler that will be called when the server is actually listening (or has failed).
   */
  listen(port: number, listenHandler: (res: HttpServer, err?: Throwable) => void): HttpServer;

  /**
   * Like listen but supplying a handler that will be called when the server is actually listening (or has failed).
   */
  listen(listenHandler: (res: HttpServer, err?: Throwable) => void): HttpServer;

  /**
   * Close the server. Any open HTTP connections will be closed.
   * <p>
   * The close happens asynchronously and the server may not be closed until some time after the call has returned.
   */
  close(): void;

  /**
   * Like close but supplying a handler that will be called when the server is actually closed (or has failed).
   */
  close(completionHandler: (res: void, err?: Throwable) => void): void;
}

declare var HttpServer: {
}
