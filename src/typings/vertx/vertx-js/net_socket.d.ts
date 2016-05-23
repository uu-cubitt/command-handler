/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./write_stream.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./socket_address.d.ts" />

declare module "vertx-js/net_socket" {
  export = NetSocket;
}

/**
 * Represents a socket-like interface to a TCP connection on either the
 * client or the server side.
 * <p>
 * Instances of this class are created on the client side by an NetClient
 * when a connection to a server is made, or on the server side by a NetServer
 * when a server accepts a connection.
 * <p>
 * It implements both  and  so it can be used with
 * Pump to pump data with flow control.
 */
interface NetSocket
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
  exceptionHandler(handler: (e: Throwable) => void): NetSocket;
  handler(handler: (e: Buffer) => void): NetSocket;
  pause(): NetSocket;
  resume(): NetSocket;
  endHandler(endHandler: (e: void) => void): NetSocket;
  write(data: Buffer): NetSocket;
  setWriteQueueMaxSize(maxSize: number): NetSocket;
  drainHandler(handler: (e: void) => void): NetSocket;

  /**
   * When a <code>NetSocket</code> is created it automatically registers an event handler with the event bus, the ID of that
   * handler is given by <code>writeHandlerID</code>.
   * <p>
   * Given this ID, a different event loop can send a buffer to that event handler using the event bus and
   * that buffer will be received by this instance in its own event loop and written to the underlying connection. This
   * allows you to write data to other connections which are owned by different event loops.
   */
  writeHandlerID(): string;

  /**
   * Write a String to the connection, encoded in UTF-8.
   */
  write(str: string): NetSocket;

  /**
   * Write a String to the connection, encoded using the encoding <code>enc</code>.
   */
  write(str: string, enc: string): NetSocket;

  /**
   * Tell the operating system to stream a file as specified by <code>filename</code> directly from disk to the outgoing connection,
   * bypassing userspace altogether (where supported by the underlying operating system. This is a very efficient way to stream files.
   */
  sendFile(filename: string): NetSocket;

  /**
   * Tell the operating system to stream a file as specified by <code>filename</code> directly from disk to the outgoing connection,
   * bypassing userspace altogether (where supported by the underlying operating system. This is a very efficient way to stream files.
   */
  sendFile(filename: string, offset: number): NetSocket;

  /**
   * Tell the operating system to stream a file as specified by <code>filename</code> directly from disk to the outgoing connection,
   * bypassing userspace altogether (where supported by the underlying operating system. This is a very efficient way to stream files.
   */
  sendFile(filename: string, offset: number, length: number): NetSocket;

  /**
   * Same as sendFile but also takes a handler that will be called when the send has completed or
   * a failure has occurred
   */
  sendFile(filename: string, resultHandler: (res: void, err?: Throwable) => void): NetSocket;

  /**
   * Same as sendFile but also takes a handler that will be called when the send has completed or
   * a failure has occurred
   */
  sendFile(filename: string, offset: number, resultHandler: (res: void, err?: Throwable) => void): NetSocket;

  /**
   * Same as sendFile but also takes a handler that will be called when the send has completed or
   * a failure has occurred
   */
  sendFile(filename: string, offset: number, length: number, resultHandler: (res: void, err?: Throwable) => void): NetSocket;

  /**
   * @return the remote address for this socket
   */
  remoteAddress(): SocketAddress;

  /**
   * @return the local address for this socket
   */
  localAddress(): SocketAddress;

  /**
   * Calls close
   */
  end(): void;

  /**
   * Close the NetSocket
   */
  close(): void;

  /**
   * Set a handler that will be called when the NetSocket is closed
   */
  closeHandler(handler: (e: void) => void): NetSocket;

  /**
   * Upgrade channel to use SSL/TLS. Be aware that for this to work SSL must be configured.
   */
  upgradeToSsl(handler: (e: void) => void): NetSocket;

  /**
   * @return true if this NetSocket is encrypted via SSL/TLS.
   */
  isSsl(): boolean;
}

declare var NetSocket: {
}
