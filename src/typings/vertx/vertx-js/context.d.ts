/// <reference path="./throwable.d.ts" />
/// <reference path="./vertx.d.ts" />
/// <reference path="./future.d.ts" />

declare module "vertx-js/context" {
  export = Context;
}

/**
 * The execution context of a Handler execution.
 * <p>
 * When Vert.x provides an event to a handler or calls the start or stop methods of a Verticle,
 * the execution is associated with a <code>Context</code>.
 * <p>
 * Usually a context is an *event-loop context* and is tied to a specific event loop thread. So executions for that
 * context always occur on that exact same event loop thread.
 * <p>
 * In the case of worker verticles and running inline blocking code a worker context will be associated with the execution
 * which will use a thread from the worker thread pool.
 * <p>
 * When a handler is set by a thread associated with a specific context, the Vert.x will guarantee that when that handler
 * is executed, that execution will be associated with the same context.
 * <p>
 * If a handler is set by a thread not associated with a context (i.e. a non Vert.x thread). Then a new context will
 * be created for that handler.
 * <p>
 * In other words, a context is propagated.
 * <p>
 * This means that when a verticle is deployed, any handlers it sets will be associated with the same context - the context
 * of the verticle.
 * <p>
 * This means (in the case of a standard verticle) that the verticle code will always be executed with the exact same
 * thread, so you don't have to worry about multi-threaded acccess to the verticle state and you can code your application
 * as single threaded.
 * <p>
 * This class also allows arbitrary data to be put and get on the context so it can be shared easily
 * amongst different handlers of, for example, a verticle instance.
 * <p>
 * This class also provides runOnContext which allows an action to be executed asynchronously using the same context.
 */
interface Context
{

  /**
   * Run the specified action asynchronously on the same context, some time after the current execution has completed.
   */
  runOnContext(action: (e: void) => void): void;

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
   */
  executeBlocking(blockingCodeHandler: (e: Future) => void, ordered: boolean, resultHandler: (res: any, err?: Throwable) => void): void;

  /**
   * Invoke executeBlocking with order = true.
   */
  executeBlocking(blockingCodeHandler: (e: Future) => void, resultHandler: (res: any, err?: Throwable) => void): void;

  /**
   * If the context is associated with a Verticle deployment, this returns the deployment ID of that deployment.
   */
  deploymentID(): string;

  /**
   * If the context is associated with a Verticle deployment, this returns the configuration that was specified when
   * the verticle was deployed.
   */
  config(): any;

  /**
   * The process args
   */
  processArgs(): Array<string>;

  /**
   * Is the current context an event loop context?
   * <p>
   * NOTE! when running blocking code using executeBlocking from a
   * standard (not worker) verticle, the context will still an event loop context and this 
   * will return true.
   */
  isEventLoopContext(): boolean;

  /**
   * Is the current context a worker context?
   * <p>
   * NOTE! when running blocking code using executeBlocking from a
   * standard (not worker) verticle, the context will still an event loop context and this 
   * will return false.
   */
  isWorkerContext(): boolean;

  /**
   * Is the current context a multi-threaded worker context?
   */
  isMultiThreadedWorkerContext(): boolean;

  /**
   * Get some data from the context.
   */
  get(key: string): any;

  /**
   * Put some data in the context.
   * <p>
   * This can be used to share data between different handlers that share a context
   */
  put(key: string, value: any): void;

  /**
   * Remove some data from the context.
   */
  remove(key: string): boolean;

  /**
   * @return The Vertx instance that created the context
   */
  owner(): Vertx;

  /**
   * @return  the number of instances of the verticle that were deployed in the deployment (if any) related
   * to this context
   */
  getInstanceCount(): number;
}

declare var Context: {

  /**
   * Is the current thread a worker thread?
   * <p>
   * NOTE! This is not always the same as calling isWorkerContext. If you are running blocking code
   * from an event loop context, then this will return true but isWorkerContext will return false.
   */
  isOnWorkerThread(): boolean;

  /**
   * Is the current thread an event thread?
   * <p>
   * NOTE! This is not always the same as calling isEventLoopContext. If you are running blocking code
   * from an event loop context, then this will return false but isEventLoopContext will return true.
   */
  isOnEventLoopThread(): boolean;

  /**
   * Is the current thread a Vert.x thread? That's either a worker thread or an event loop thread
   */
  isOnVertxThread(): boolean;
}
