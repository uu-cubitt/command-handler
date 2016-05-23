/// <reference path="./throwable.d.ts" />

declare module "vertx-js/socket_address" {
  export = SocketAddress;
}

/**
 * The address of a socket
 */
interface SocketAddress
{
  host(): string;
  port(): number;
}

declare var SocketAddress: {
}
