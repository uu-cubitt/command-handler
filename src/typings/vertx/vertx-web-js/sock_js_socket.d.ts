/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./session.d.ts" />
/// <reference path="../vertx-auth-common-js/user.d.ts" />
/// <reference path="../vertx-js/buffer.d.ts" />
/// <reference path="../vertx-js/write_stream.d.ts" />
/// <reference path="../vertx-js/read_stream.d.ts" />
/// <reference path="../vertx-js/multi_map.d.ts" />
/// <reference path="../vertx-js/socket_address.d.ts" />

declare module "vertx-web-js/sock_js_socket" {
  export = SockJSSocket;
}

/**
 *
 * You interact with SockJS clients through instances of SockJS socket.
 * <p>
 * The API is very similar to WebSocket.
 * It implements both  and 
 * so it can be used with
 * Pump to pump data with flow control.<p>
 */
interface SockJSSocket
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
  exceptionHandler(handler: (e: Throwable) => void): SockJSSocket;
  handler(handler: (e: Buffer) => void): SockJSSocket;
  pause(): SockJSSocket;
  resume(): SockJSSocket;
  endHandler(endHandler: (e: void) => void): SockJSSocket;
  write(data: Buffer): SockJSSocket;
  setWriteQueueMaxSize(maxSize: number): SockJSSocket;
  drainHandler(handler: (e: void) => void): SockJSSocket;

  /**
   * When a <code>SockJSSocket</code> is created it automatically registers an event handler with the event bus, the ID of that
   * handler is given by <code>writeHandlerID</code>.
   * <p>
   * Given this ID, a different event loop can send a buffer to that event handler using the event bus and
   * that buffer will be received by this instance in its own event loop and written to the underlying socket. This
   * allows you to write data to other sockets which are owned by different event loops.
   */
  writeHandlerID(): string;

  /**
   * Call end.
   */
  end(): void;

  /**
   * Close it
   */
  close(): void;

  /**
   * Return the remote address for this socket
   */
  remoteAddress(): SocketAddress;

  /**
   * Return the local address for this socket
   */
  localAddress(): SocketAddress;

  /**
   * Return the headers corresponding to the last request for this socket or the websocket handshake
   * Any cookie headers will be removed for security reasons
   */
  headers(): MultiMap;

  /**
   * Return the URI corresponding to the last request for this socket or the websocket handshake
   */
  uri(): string;

  /**
   * @return the Vert.x-Web session corresponding to this socket
   */
  webSession(): Session;

  /**
   *  @return the Vert.x-Web user corresponding to this socket
   */
  webUser(): User;
}

declare var SockJSSocket: {
}
