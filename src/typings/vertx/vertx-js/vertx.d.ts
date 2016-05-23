/// <reference path="./throwable.d.ts" />
/// <reference path="./console.d.ts" />
/// <reference path="./datagram_socket.d.ts" />
/// <reference path="./http_server.d.ts" />
/// <reference path="./context.d.ts" />
/// <reference path="./future.d.ts" />
/// <reference path="./shared_data.d.ts" />
/// <reference path="./timeout_stream.d.ts" />
/// <reference path="./dns_client.d.ts" />
/// <reference path="./event_bus.d.ts" />
/// <reference path="./measured.d.ts" />
/// <reference path="./net_server.d.ts" />
/// <reference path="./file_system.d.ts" />
/// <reference path="./net_client.d.ts" />
/// <reference path="./http_client.d.ts" />

declare module "vertx-js/vertx" {
  export = Vertx;
}

/**
 * The entry point into the Vert.x Core API.
 * <p>
 * You use an instance of this class for functionality including:
 * <ul>
 *   <li>Creating TCP clients and servers</li>
 *   <li>Creating HTTP clients and servers</li>
 *   <li>Creating DNS clients</li>
 *   <li>Creating Datagram sockets</li>
 *   <li>Setting and cancelling periodic and one-shot timers</li>
 *   <li>Getting a reference to the event bus API</li>
 *   <li>Getting a reference to the file system API</li>
 *   <li>Getting a reference to the shared data API</li>
 *   <li>Deploying and undeploying verticles</li>
 * </ul>
 * <p>
 * Most functionality in Vert.x core is fairly low level.
 * <p>
 * To create an instance of this class you can use the static factory methods: vertx,
 * vertx and clusteredVertx.
 * <p>
 * Please see the user manual for more detailed usage information.
 */
interface Vertx
  extends
    Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;

  /**
   * Gets the current context, or creates one if there isn't one
   */
  getOrCreateContext(): Context;

  /**
   * Create a TCP/SSL server using the specified options
   */
  createNetServer(options: any): NetServer;

  /**
   * Create a TCP/SSL server using default options
   */
  createNetServer(): NetServer;

  /**
   * Create a TCP/SSL client using the specified options
   */
  createNetClient(options: any): NetClient;

  /**
   * Create a TCP/SSL client using default options
   */
  createNetClient(): NetClient;

  /**
   * Create an HTTP/HTTPS server using the specified options
   */
  createHttpServer(options: any): HttpServer;

  /**
   * Create an HTTP/HTTPS server using default options
   */
  createHttpServer(): HttpServer;

  /**
   * Create a HTTP/HTTPS client using the specified options
   */
  createHttpClient(options: any): HttpClient;

  /**
   * Create a HTTP/HTTPS client using default options
   */
  createHttpClient(): HttpClient;

  /**
   * Create a datagram socket using the specified options
   */
  createDatagramSocket(options: any): DatagramSocket;

  /**
   * Create a datagram socket using default options
   */
  createDatagramSocket(): DatagramSocket;

  /**
   * Get the filesystem object. There is a single instance of FileSystem per Vertx instance.
   */
  fileSystem(): FileSystem;

  /**
   * Get the event bus object. There is a single instance of EventBus per Vertx instance.
   */
  eventBus(): EventBus;

  /**
   * Create a DNS client to connect to a DNS server at the specified host and port
   */
  createDnsClient(port: number, host: string): DnsClient;

  /**
   * Get the shared data object. There is a single instance of SharedData per Vertx instance.
   */
  sharedData(): SharedData;

  /**
   * Set a one-shot timer to fire after <code>delay</code> milliseconds, at which point <code>handler</code> will be called with
   * the id of the timer.
   */
  setTimer(delay: number, handler: (e: number) => void): number;

  /**
   * Returns a one-shot timer as a read stream. The timer will be fired after <code>delay</code> milliseconds after
   * the  has been called.
   */
  timerStream(delay: number): TimeoutStream;

  /**
   * Set a periodic timer to fire every <code>delay</code> milliseconds, at which point <code>handler</code> will be called with
   * the id of the timer.
   */
  setPeriodic(delay: number, handler: (e: number) => void): number;

  /**
   * Returns a periodic timer as a read stream. The timer will be fired every <code>delay</code> milliseconds after
   * the  has been called.
   */
  periodicStream(delay: number): TimeoutStream;

  /**
   * Cancels the timer with the specified <code>id</code>.
   */
  cancelTimer(id: number): boolean;

  /**
   * Puts the handler on the event queue for the current context so it will be run asynchronously ASAP after all
   * preceeding events have been handled.
   */
  runOnContext(action: (e: void) => void): void;

  /**
   * Stop the the Vertx instance and release any resources held by it.
   * <p>
   * The instance cannot be used after it has been closed.
   * <p>
   * The actual close is asynchronous and may not complete until after the call has returned.
   */
  close(): void;

  /**
   * Like close but the completionHandler will be called when the close is complete
   */
  close(completionHandler: (res: void, err?: Throwable) => void): void;

  /**
   * Deploy a verticle instance given a name.
   * <p>
   * Given the name, Vert.x selects a  instance to use to instantiate the verticle.
   * <p>
   * For the rules on how factories are selected please consult the user manual.
   */
  deployVerticle(name: string): void;

  /**
   * Like deployVerticle but the completionHandler will be notified when the deployment is complete.
   * <p>
   * If the deployment is successful the result will contain a String representing the unique deployment ID of the
   * deployment.
   * <p>
   * This deployment ID can subsequently be used to undeploy the verticle.
   */
  deployVerticle(name: string, completionHandler: (res: string, err?: Throwable) => void): void;

  /**
   * Like deployVerticle but DeploymentOptions are provided to configure the
   * deployment.
   */
  deployVerticle(name: string, options: any): void;

  /**
   * Like deployVerticle but DeploymentOptions are provided to configure the
   * deployment.
   */
  deployVerticle(name: string, options: any, completionHandler: (res: string, err?: Throwable) => void): void;

  /**
   * Undeploy a verticle deployment.
   * <p>
   * The actual undeployment happens asynchronously and may not complete until after the method has returned.
   */
  undeploy(deploymentID: string): void;

  /**
   * Like #undeploy(String)  but the completionHandler will be notified when the undeployment is complete.
   */
  undeploy(deploymentID: string, completionHandler: (res: void, err?: Throwable) => void): void;

  /**
   * Return a Set of deployment IDs for the currently deployed deploymentIDs.
   */
  deploymentIDs(): Array<string>;

  /**
   * Is this Vert.x instance clustered?
   */
  isClustered(): boolean;

  /**
   * Safely execute some blocking code.
   * <p>
   * Executes the blocking code in the handler <code>blockingCodeHandler</code> using a thread from the worker pool.
   * <p>
   * When the code is complete the handler <code>resultHandler</code> will be called with the result on the original context
   * (e.g. on the original event loop of the caller).
   * <p>
   * A <code>Future</code> instance is passed into <code>blockingCodeHandler</code>. When the blocking code successfully completes,
   * the handler should call the complete or complete method, or the fail
   * method if it failed.
   * <p>
   * In the <code>blockingCodeHandler</code> the current context remains the original context and therefore any task
   * scheduled in the <code>blockingCodeHandler</code> will be executed on the this context and not on the worker thread.
   */
  executeBlocking(blockingCodeHandler: (e: Future) => void, ordered: boolean, resultHandler: (res: any, err?: Throwable) => void): void;

  /**
   * Like executeBlocking called with ordered = true.
   */
  executeBlocking(blockingCodeHandler: (e: Future) => void, resultHandler: (res: any, err?: Throwable) => void): void;
}

declare var Vertx: {

  /**
   * Creates a non clustered instance using default options.
   */
  vertx(): Vertx;

  /**
   * Creates a non clustered instance using the specified options
   */
  vertx(options: any): Vertx;

  /**
   * Creates a clustered instance using the specified options.
   * <p>
   * The instance is created asynchronously and the resultHandler is called with the result when it is ready.
   */
  clusteredVertx(options: any, resultHandler: (res: Vertx, err?: Throwable) => void): void;

  /**
   * Gets the current context
   */
  currentContext(): Context;
}

declare var vertx: Vertx;

declare var console: Console;
