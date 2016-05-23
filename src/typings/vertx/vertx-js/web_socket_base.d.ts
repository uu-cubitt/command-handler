/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./write_stream.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./web_socket_frame.d.ts" />
/// <reference path="./socket_address.d.ts" />

declare module "vertx-js/web_socket_base" {
  export = WebSocketBase;
}

/**
 * Base WebSocket implementation.
 * <p>
 * It implements both  and  so it can be used with
 * Pump to pump data with flow control.
 */
interface WebSocketBase
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
  exceptionHandler(handler: (e: Throwable) => void): WebSocketBase;
  handler(handler: (e: Buffer) => void): WebSocketBase;
  pause(): WebSocketBase;
  resume(): WebSocketBase;
  endHandler(endHandler: (e: void) => void): WebSocketBase;
  write(data: Buffer): WebSocketBase;
  setWriteQueueMaxSize(maxSize: number): WebSocketBase;
  drainHandler(handler: (e: void) => void): WebSocketBase;

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
   * Write a WebSocket frame to the connection
   */
  writeFrame(frame: WebSocketFrame): WebSocketBase;

  /**
   * Write a final WebSocket text frame to the connection
   */
  writeFinalTextFrame(text: string): WebSocketBase;

  /**
   * Write a final WebSocket binary frame to the connection
   */
  writeFinalBinaryFrame(data: Buffer): WebSocketBase;

  /**
   * Writes a (potentially large) piece of binary data to the connection. This data might be written as multiple frames
   * if it exceeds the maximum WebSocket frame size.
   */
  writeBinaryMessage(data: Buffer): WebSocketBase;

  /**
   * Set a close handler. This will be called when the WebSocket is closed.
   */
  closeHandler(handler: (e: void) => void): WebSocketBase;

  /**
   * Set a frame handler on the connection. This handler will be called when frames are read on the connection.
   */
  frameHandler(handler: (e: WebSocketFrame) => void): WebSocketBase;

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
}

declare var WebSocketBase: {
}
