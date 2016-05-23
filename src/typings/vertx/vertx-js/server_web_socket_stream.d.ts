/// <reference path="./throwable.d.ts" />
/// <reference path="./server_web_socket.d.ts" />
/// <reference path="./read_stream.d.ts" />

declare module "vertx-js/server_web_socket_stream" {
  export = ServerWebSocketStream;
}

/**
 * A ReadStream of ServerWebSocket, used for
 * notifying web socket connections to a HttpServer.
 */
interface ServerWebSocketStream
  extends
    ReadStream
{
  exceptionHandler(handler: (e: Throwable) => void): ServerWebSocketStream;
  handler(handler: (e: ServerWebSocket) => void): ServerWebSocketStream;
  pause(): ServerWebSocketStream;
  resume(): ServerWebSocketStream;
  endHandler(endHandler: (e: void) => void): ServerWebSocketStream;
}

declare var ServerWebSocketStream: {
}
