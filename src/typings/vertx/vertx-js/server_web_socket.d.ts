/// <reference path="./throwable.d.ts" />
/// <reference path="./web_socket_base.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./multi_map.d.ts" />
/// <reference path="./web_socket_frame.d.ts" />
/// <reference path="./socket_address.d.ts" />

declare module "vertx-js/server_web_socket" {
  export = ServerWebSocket;
}

/**
 * Represents a server side WebSocket.
 * <p>
 * Instances of this class are passed into a websocketHandler or provided
 * when a WebSocket handshake is manually upgradeed.
 */
interface ServerWebSocket
  extends
    WebSocketBase
{

  /**
   * Same as end but writes some data to the stream before ending.
   */
  end(t: Buffer): void;

  /**
   * This will return <code>true</code> if there are more bytes in the write queue than the value set using setWriteQueueMaxSize
   */
  writeQueueFull(): boolean;

  /**
   * When a <code>Websocket</code> is created it automatically registers an event handler with the event bus - the ID of that
   * handler is given by this method.
   * <p>
   * Given this ID, a different event loop can send a binary frame to that event handler using the event bus and
   * that buffer will be received by this instance in its own event loop and written to the underlying connection. This
   * allows you to write data to other WebSockets which are owned by different event loops.
   */
  binaryHandlerID(): string;

  /**
   * When a <code>Websocket</code> is created it automatically registers an event handler with the eventbus, the ID of that
   * handler is given by <code>textHandlerID</code>.
   * <p>
   * Given this ID, a different event loop can send a text frame to that event handler using the event bus and
   * that buffer will be received by this instance in its own event loop and written to the underlying connection. This
   * allows you to write data to other WebSockets which are owned by different event loops.
   */
  textHandlerID(): string;

  /**
   * Calls close
   */
  end(): void;

  /**
   * Close the WebSocket.
   */
  close(): void;

  /**
   * @return the remote address for this socket
   */
  remoteAddress(): SocketAddress;

  /**
   * @return the local address for this socket
   */
  localAddress(): SocketAddress;
  exceptionHandler(handler: (e: Throwable) => void): ServerWebSocket;
  handler(handler: (e: Buffer) => void): ServerWebSocket;
  pause(): ServerWebSocket;
  resume(): ServerWebSocket;
  endHandler(endHandler: (e: void) => void): ServerWebSocket;
  write(data: Buffer): ServerWebSocket;
  setWriteQueueMaxSize(maxSize: number): ServerWebSocket;
  drainHandler(handler: (e: void) => void): ServerWebSocket;
  writeFrame(frame: WebSocketFrame): ServerWebSocket;
  writeFinalTextFrame(text: string): ServerWebSocket;
  writeFinalBinaryFrame(data: Buffer): ServerWebSocket;
  writeBinaryMessage(data: Buffer): ServerWebSocket;
  closeHandler(handler: (e: void) => void): ServerWebSocket;
  frameHandler(handler: (e: WebSocketFrame) => void): ServerWebSocket;
  uri(): string;

  /**
   * @return the WebSocket handshake path.
   */
  path(): string;

  /**
   * @return the WebSocket handshake query string.
   */
  query(): string;

  /**
   * @return the headers in the WebSocket handshake
   */
  headers(): MultiMap;

  /**
   * Reject the WebSocket.
   * <p>
   * Calling this method from the websocket handler when it is first passed to you gives you the opportunity to reject
   * the websocket, which will cause the websocket handshake to fail by returning
   * a 404 response code.
   * <p>
   * You might use this method, if for example you only want to accept WebSockets with a particular path.
   */
  reject(): void;
}

declare var ServerWebSocket: {
}
