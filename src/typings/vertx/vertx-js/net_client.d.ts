/// <reference path="./throwable.d.ts" />
/// <reference path="./measured.d.ts" />
/// <reference path="./net_socket.d.ts" />

declare module "vertx-js/net_client" {
  export = NetClient;
}

/**
 * A TCP client.
 * <p>
 * Multiple connections to different servers can be made using the same instance.
 * <p>
 * This client supports a configurable number of connection attempts and a configurable
 * delay between attempts.
 */
interface NetClient
  extends
    Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;

  /**
   * Open a connection to a server at the specific <code>port</code> and <code>host</code>.
   * <p>
   * <code>host</code> can be a valid host name or IP address. The connect is done asynchronously and on success, a
   * NetSocket instance is supplied via the <code>connectHandler</code> instance
   */
  connect(port: number, host: string, connectHandler: (res: NetSocket, err?: Throwable) => void): NetClient;

  /**
   * Close the client.
   * <p>
   * Any sockets which have not been closed manually will be closed here. The close is asynchronous and may not
   * complete until some time after the method has returned.
   */
  close(): void;
}

declare var NetClient: {
}
