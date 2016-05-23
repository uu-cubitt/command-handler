/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./shell_server.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-shell-js/shell_service" {
  export = ShellService;
}

/**
 * The shell service, provides a remotely accessible shell available via Telnet or SSH according to the
 * ShellServiceOptions configuration.<p/>
 *
 * The shell service will expose commands using CommandResolver on the classpath and
 * the shared command registry for the Vert.x instance.
 */
interface ShellService
{

  /**
   * Start the shell service, this is an asynchronous start.
   */
  start(): void;

  /**
   * Start the shell service, this is an asynchronous start.
   */
  start(startHandler: (res: void, err?: Throwable) => void): void;

  /**
   * @return the shell server
   */
  server(): ShellServer;

  /**
   * Stop the shell service, this is an asynchronous stop.
   */
  stop(): void;

  /**
   * Stop the shell service, this is an asynchronous start.
   */
  stop(stopHandler: (res: void, err?: Throwable) => void): void;
}

declare var ShellService: {

  /**
   * Like create, with default options.
   */
  create(vertx: Vertx): ShellService;

  /**
   * Create a new shell service.
   */
  create(vertx: Vertx, options: any): ShellService;
}
