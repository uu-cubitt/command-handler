/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./sock_js_socket.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./router.d.ts" />
/// <reference path="./routing_context.d.ts" />
/// <reference path="./bridge_event.d.ts" />

declare module "vertx-web-js/sock_js_handler" {
  export = SockJSHandler;
}

/**
 *
 * A handler that allows you to handle SockJS connections from clients.
 * <p>
 * We currently support version 0.3.3 of the SockJS protocol, which can be found in
 * <a href="https://github.com/sockjs/sockjs-protocol/tree/v0.3.3">this tag:</a>
 */
interface SockJSHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;

  /**
   * Set a SockJS socket handler. This handler will be called with a SockJS socket whenever a SockJS connection
   * is made from a client
   */
  socketHandler(handler: (e: SockJSSocket) => void): SockJSHandler;

  /**
   * Bridge the SockJS handler to the Vert.x event bus. This basically installs a built-in SockJS socket handler
   * which takes SockJS traffic and bridges it to the event bus, thus allowing you to extend the server-side
   * Vert.x event bus to browsers
   */
  bridge(bridgeOptions: any): SockJSHandler;

  /**
   * Like bridge but specifying a handler
   * that will receive bridge events.
   */
  bridge(bridgeOptions: any, bridgeEventHandler: (e: BridgeEvent) => void): SockJSHandler;
}

declare var SockJSHandler: {

  /**
   * Create a SockJS handler
   */
  create(vertx: Vertx): SockJSHandler;

  /**
   * Create a SockJS handler
   */
  create(vertx: Vertx, options: any): SockJSHandler;

  /**
   * Install SockJS test applications on a router - used when running the SockJS test suite
   */
  installTestApplications(router: Router, vertx: Vertx): void;
}
