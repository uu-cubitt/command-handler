/// <reference path="./throwable.d.ts" />
/// <reference path="./net_socket_stream.d.ts" />
/// <reference path="./measured.d.ts" />
/// <reference path="./net_socket.d.ts" />

declare module "vertx-js/net_server" {
  export = NetServer;
}

/**
 * Represents a TCP server
 */
interface NetServer
  extends
    Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;

  /**
   * Return the connect stream for this server. The server can only have at most one handler at any one time.
   * As the server accepts TCP or SSL connections it creates an instance of NetSocket and passes it to the
   * connect stream .
   */
  connectStream(): NetSocketStream;

  /**
   * Supply a connect handler for this server. The server can only have at most one connect handler at any one time.
   * As the server accepts TCP or SSL connections it creates an instance of NetSocket and passes it to the
   * connect handler.
   */
  connectHandler(handler: (e: NetSocket) => void): NetServer;

  /**
   * Start listening on the port and host as configured in the NetServerOptions used when
   * creating the server.
   * <p>
   * The server may not be listening until some time after the call to listen has returned.
   */
  listen(): NetServer;

  /**
   * Like listen but providing a handler that will be notified when the server is listening, or fails.
   */
  listen(listenHandler: (res: NetServer, err?: Throwable) => void): NetServer;

  /**
   * Start listening on the specified port and host, ignoring post and host configured in the NetServerOptions used when
   * creating the server.
   * <p>
   * Port <code>0</code> can be specified meaning "choose an random port".
   * <p>
   * Host <code>0.0.0.0</code> can be specified meaning "listen on all available interfaces".
   * <p>
   * The server may not be listening until some time after the call to listen has returned.
   */
  listen(port: number, host: string): NetServer;

  /**
   * Like listen but providing a handler that will be notified when the server is listening, or fails.
   */
  listen(port: number, host: string, listenHandler: (res: NetServer, err?: Throwable) => void): NetServer;

  /**
   * Start listening on the specified port and host "0.0.0.0", ignoring post and host configured in the
   * NetServerOptions used when creating the server.
   * <p>
   * Port <code>0</code> can be specified meaning "choose an random port".
   * <p>
   * The server may not be listening until some time after the call to listen has returned.
   */
  listen(port: number): NetServer;

  /**
   * Like listen but providing a handler that will be notified when the server is listening, or fails.
   */
  listen(port: number, listenHandler: (res: NetServer, err?: Throwable) => void): NetServer;

  /**
   * Close the server. This will close any currently open connections. The close may not complete until after this
   * method has returned.
   */
  close(): void;

  /**
   * Like close but supplying a handler that will be notified when close is complete.
   */
  close(completionHandler: (res: void, err?: Throwable) => void): void;

  /**
   * The actual port the server is listening on. This is useful if you bound the server specifying 0 as port number
   * signifying an ephemeral port
   */
  actualPort(): number;
}

declare var NetServer: {
}
