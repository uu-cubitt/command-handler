/// <reference path="./throwable.d.ts" />
/// <reference path="./packet_writestream.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./datagram_packet.d.ts" />
/// <reference path="./measured.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./socket_address.d.ts" />

declare module "vertx-js/datagram_socket" {
  export = DatagramSocket;
}

/**
 * A datagram socket can be used to send DatagramPacket's to remote datagram servers
 * and receive DatagramPackets .
 * <p>
 * Usually you use a datagram socket to send UDP over the wire. UDP is connection-less which means you are not connected
 * to the remote peer in a persistent way. Because of this you have to supply the address and port of the remote peer
 * when sending data.
 * <p>
 * You can send data to ipv4 or ipv6 addresses, which also include multicast addresses.
 * <p>
 * Please consult the documentation for more information on datagram sockets.
 */
interface DatagramSocket
  extends
    ReadStream
  , 
    Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;

  /**
   * Write the given Buffer to the SocketAddress.
   * The Handler will be notified once the write completes.
   */
  send(packet: Buffer, port: number, host: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Returns a PacketWritestream able to send  to the
   * SocketAddress.
   */
  sender(port: number, host: string): PacketWritestream;

  /**
   * Write the given String to the SocketAddress using UTF8 encoding.
   * The  will be notified once the write completes.
   */
  send(str: string, port: number, host: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Write the given String to the SocketAddress using the given encoding.
   * The  will be notified once the write completes.
   */
  send(str: string, enc: string, port: number, host: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Closes the DatagramSocket implementation asynchronous
   * and notifies the handler once done.
   */
  close(handler: (res: void, err?: Throwable) => void): void;

  /**
   * Closes the DatagramSocket. The close itself is asynchronous.
   */
  close(): void;

  /**
   * Return the SocketAddress to which
   * this DatagramSocket is bound.
   */
  localAddress(): SocketAddress;

  /**
   * Joins a multicast group and listens for packets send to it.
   * The  is notified once the operation completes.
   */
  listenMulticastGroup(multicastAddress: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Joins a multicast group and listens for packets send to it on the given network interface.
   * The  is notified once the operation completes.
   */
  listenMulticastGroup(multicastAddress: string, networkInterface: string, source: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Leaves a multicast group and stops listening for packets send to it.
   * The  is notified once the operation completes.
   */
  unlistenMulticastGroup(multicastAddress: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Leaves a multicast group and stops listening for packets send to it on the given network interface.
   * The  is notified once the operation completes.
   */
  unlistenMulticastGroup(multicastAddress: string, networkInterface: string, source: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Block the given address for the given multicast address and notifies the  once
   * the operation completes.
   */
  blockMulticastGroup(multicastAddress: string, sourceToBlock: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Block the given address for the given multicast address on the given network interface and notifies
   * the  once the operation completes.
   */
  blockMulticastGroup(multicastAddress: string, networkInterface: string, sourceToBlock: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;

  /**
   * Start listening on the given port and host. The handler will be called when the socket is listening.
   */
  listen(port: number, host: string, handler: (res: DatagramSocket, err?: Throwable) => void): DatagramSocket;
  pause(): DatagramSocket;
  resume(): DatagramSocket;
  endHandler(endHandler: (e: void) => void): DatagramSocket;
  handler(handler: (e: DatagramPacket) => void): DatagramSocket;
  exceptionHandler(handler: (e: Throwable) => void): DatagramSocket;
}

declare var DatagramSocket: {
}
