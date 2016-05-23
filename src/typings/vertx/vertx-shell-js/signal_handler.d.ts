/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-shell-js/signal_handler" {
  export = SignalHandler;
}

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
interface SignalHandler
{
  deliver(key: number): boolean;
}

declare var SignalHandler: {
}
