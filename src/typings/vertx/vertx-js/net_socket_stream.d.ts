/// <reference path="./throwable.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./net_socket.d.ts" />

declare module "vertx-js/net_socket_stream" {
  export = NetSocketStream;
}

/**
 * A ReadStream of NetSocket, used for notifying
 * socket connections to a NetServer.
 */
interface NetSocketStream
  extends
    ReadStream
{
  exceptionHandler(handler: (e: Throwable) => void): NetSocketStream;
  handler(handler: (e: NetSocket) => void): NetSocketStream;
  pause(): NetSocketStream;
  resume(): NetSocketStream;
  endHandler(endHandler: (e: void) => void): NetSocketStream;
}

declare var NetSocketStream: {
}
