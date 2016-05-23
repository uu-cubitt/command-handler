/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-web-js/router.d.ts" />
/// <reference path="./term.d.ts" />
/// <reference path="../vertx-auth-common-js/auth_provider.d.ts" />

declare module "vertx-shell-js/term_server" {
  export = TermServer;
}

/**
 * A server for terminal based applications.
 */
interface TermServer
{

  /**
   * Set the term handler that will receive incoming client connections. When a remote terminal connects
   * the <code>handler</code> will be called with the Term which can be used to interact with the remote
   * terminal.
   */
  termHandler(handler: (e: Term) => void): TermServer;

  /**
   * Set an auth provider to use, any provider configured in options will override this provider. This should be used
   * when a custom auth provider should be used.
   */
  authProvider(provider: AuthProvider): TermServer;

  /**
   * Bind the term server, the termHandler must be set before.
   */
  listen(): TermServer;

  /**
   * Bind the term server, the termHandler must be set before.
   */
  listen(listenHandler: (res: TermServer, err?: Throwable) => void): TermServer;

  /**
   * The actual port the server is listening on. This is useful if you bound the server specifying 0 as port number
   * signifying an ephemeral port
   */
  actualPort(): number;

  /**
   * Close the server. This will close any currently open connections. The close may not complete until after this
   * method has returned.
   */
  close(): void;

  /**
   * Like close but supplying a handler that will be notified when close is complete.
   */
  close(completionHandler: (res: void, err?: Throwable) => void): void;
}

declare var TermServer: {

  /**
   * Create a term server for the SSH protocol.
   */
  createSSHTermServer(vertx: Vertx): TermServer;

  /**
   * Create a term server for the SSH protocol.
   */
  createSSHTermServer(vertx: Vertx, options: any): TermServer;

  /**
   * Create a term server for the Telnet protocol.
   */
  createTelnetTermServer(vertx: Vertx): TermServer;

  /**
   * Create a term server for the Telnet protocol.
   */
  createTelnetTermServer(vertx: Vertx, options: any): TermServer;

  /**
   * Create a term server for the HTTP protocol.
   */
  createHttpTermServer(vertx: Vertx): TermServer;

  /**
   * Create a term server for the HTTP protocol.
   */
  createHttpTermServer(vertx: Vertx, options: any): TermServer;

  /**
   * Create a term server for the HTTP protocol, using an existing router.
   */
  createHttpTermServer(vertx: Vertx, router: Router): TermServer;

  /**
   * Create a term server for the HTTP protocol, using an existing router.
   */
  createHttpTermServer(vertx: Vertx, router: Router, options: any): TermServer;
}
