/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./socket_address.d.ts" />

declare module "vertx-js/datagram_packet" {
  export = DatagramPacket;
}

/**
 * A received datagram packet (UDP) which contains the data and information about the sender of the data itself.
 */
interface DatagramPacket
{

  /**
   * Returns the SocketAddress of the sender that sent
   * this DatagramPacket.
   */
  sender(): SocketAddress;

  /**
   * Returns the data of the DatagramPacket
   */
  data(): Buffer;
}

declare var DatagramPacket: {
}
