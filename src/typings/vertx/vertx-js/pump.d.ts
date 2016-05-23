/// <reference path="./throwable.d.ts" />
/// <reference path="./write_stream.d.ts" />
/// <reference path="./read_stream.d.ts" />

declare module "vertx-js/pump" {
  export = Pump;
}

/**
 * Pumps data from a ReadStream to a WriteStream and performs flow control where necessary to
 * prevent the write stream buffer from getting overfull.
 * <p>
 * Instances of this class read items from a ReadStream and write them to a WriteStream. If data
 * can be read faster than it can be written this could result in the write queue of the WriteStream growing
 * without bound, eventually causing it to exhaust all available RAM.
 * <p>
 * To prevent this, after each write, instances of this class check whether the write queue of the WriteStream is full, and if so, the ReadStream is paused, and a <code>drainHandler</code> is set on the
 * WriteStream.
 * <p>
 * When the WriteStream has processed half of its backlog, the <code>drainHandler</code> will be
 * called, which results in the pump resuming the ReadStream.
 * <p>
 * This class can be used to pump from any ReadStream to any WriteStream,
 * e.g. from an HttpServerRequest to an AsyncFile,
 * or from NetSocket to a WebSocket.
 * <p>
 * Please see the documentation for more information.
 */
interface Pump
{

  /**
   * Set the write queue max size to <code>maxSize</code>
   */
  setWriteQueueMaxSize(maxSize: number): Pump;

  /**
   * Start the Pump. The Pump can be started and stopped multiple times.
   */
  start(): Pump;

  /**
   * Stop the Pump. The Pump can be started and stopped multiple times.
   */
  stop(): Pump;

  /**
   * Return the total number of items pumped by this pump.
   */
  numberPumped(): number;
}

declare var Pump: {

  /**
   * Create a new <code>Pump</code> with the given <code>ReadStream</code> and <code>WriteStream</code>
   */
  pump(rs: ReadStream, ws: WriteStream): Pump;

  /**
   * Create a new <code>Pump</code> with the given <code>ReadStream</code> and <code>WriteStream</code> and
   * <code>writeQueueMaxSize</code>
   */
  pump(rs: ReadStream, ws: WriteStream, writeQueueMaxSize: number): Pump;
}
