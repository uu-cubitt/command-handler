/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./term_server.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./shell.d.ts" />
/// <reference path="./command_resolver.d.ts" />
/// <reference path="./term.d.ts" />

declare module "vertx-shell-js/shell_server" {
  export = ShellServer;
}

/**
 * The shell server.<p/>
 *
 * A shell server is associated with a collection of : the registerTermServer
 * method registers a term server. Term servers life cycle are managed by this server.<p/>
 *
 * When a  receives an incoming connection, a  instance is created and
 * associated with this connection.<p/>
 *
 * The createShell method can be used to create  instance for testing purposes.
 */
interface ShellServer
{

  /**
   * Register a command resolver for this server.
   */
  registerCommandResolver(resolver: CommandResolver): ShellServer;

  /**
   * Register a term server to this shell server, the term server lifecycle methods are managed by this shell server.
   */
  registerTermServer(termServer: TermServer): ShellServer;

  /**
   * Create a new shell, the returned shell should be closed explicitely.
   */
  createShell(term: Term): Shell;

  /**
   * Create a new shell, the returned shell should be closed explicitely.
   */
  createShell(): Shell;

  /**
   * Start the shell service, this is an asynchronous start.
   */
  listen(): ShellServer;

  /**
   * Start the shell service, this is an asynchronous start.
   */
  listen(listenHandler: (res: void, err?: Throwable) => void): ShellServer;

  /**
   * Close the shell server, this is an asynchronous close.
   */
  close(): void;

  /**
   * Close the shell server, this is an asynchronous close.
   */
  close(completionHandler: (res: void, err?: Throwable) => void): void;
}

declare var ShellServer: {

  /**
   * Create a new shell server with default options.
   */
  create(vertx: Vertx, options: any): ShellServer;

  /**
   * Create a new shell server with specific options.
   */
  create(vertx: Vertx): ShellServer;
}
