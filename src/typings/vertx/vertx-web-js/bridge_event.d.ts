/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./sock_js_socket.d.ts" />
/// <reference path="../vertx-js/future.d.ts" />

declare module "vertx-web-js/bridge_event" {
  export = BridgeEvent;
}

/**
 * Represents an event that occurs on the event bus bridge.
 * <p>
 * Please consult the documentation for a full explanation.
 */
interface BridgeEvent
  extends Future
{

  /**
   * @return  the type of the event
   */
  type(): any;

  /**
   * Get the raw JSON message for the event. This will be null for SOCKET_CREATED or SOCKET_CLOSED events as there is
   * no message involved.
   */
  rawMessage(): any;

  /**
   * Get the SockJSSocket instance corresponding to the event
   */
  socket(): SockJSSocket;
}

declare var BridgeEvent: {
}
