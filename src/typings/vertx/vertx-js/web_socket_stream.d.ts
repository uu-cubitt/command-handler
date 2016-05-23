/// <reference path="./throwable.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./web_socket.d.ts" />

declare module "vertx-js/web_socket_stream" {
  export = WebSocketStream;
}

/**
 * A stream for HttpClient WebSocket connection.
 * <p>
 * When the connection attempt is successful, the stream handler is called back with the WebSocket
 * argument, immediately followed by a call to the end handler. When the connection attempt fails, the exception handler is invoked.
 * <p>
 * The connection occurs when the handler method is called with a non null handler, the other handlers should be
 * set before setting the handler.
 */
interface WebSocketStream
  extends
    ReadStream
{
  exceptionHandler(handler: (e: Throwable) => void): WebSocketStream;
  handler(handler: (e: WebSocket) => void): WebSocketStream;
  pause(): WebSocketStream;
  resume(): WebSocketStream;
  endHandler(endHandler: (e: void) => void): WebSocketStream;
}

declare var WebSocketStream: {
}
