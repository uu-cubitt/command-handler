/// <reference path="./throwable.d.ts" />
/// <reference path="./web_socket_base.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./web_socket_frame.d.ts" />
/// <reference path="./socket_address.d.ts" />

declare module "vertx-js/web_socket" {
  export = WebSocket;
}

/**
 * Represents a client-side WebSocket.
 */
interface WebSocket
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
  exceptionHandler(handler: (e: Throwable) => void): WebSocket;
  handler(handler: (e: Buffer) => void): WebSocket;
  pause(): WebSocket;
  resume(): WebSocket;
  endHandler(endHandler: (e: void) => void): WebSocket;
  write(data: Buffer): WebSocket;
  setWriteQueueMaxSize(maxSize: number): WebSocket;
  drainHandler(handler: (e: void) => void): WebSocket;
  writeFrame(frame: WebSocketFrame): WebSocket;
  writeFinalTextFrame(text: string): WebSocket;
  writeFinalBinaryFrame(data: Buffer): WebSocket;
  writeBinaryMessage(data: Buffer): WebSocket;
  closeHandler(handler: (e: void) => void): WebSocket;
  frameHandler(handler: (e: WebSocketFrame) => void): WebSocket;
}

/*declare var WebSocket: {
}*/
