/// <reference path="./throwable.d.ts" />
/// <reference path="./message.d.ts" />

declare module "vertx-js/send_context" {
  export = SendContext;
}

/**
 *
 * Encapsulates a message being sent from Vert.x. Used with event bus interceptors
 */
interface SendContext
{

  /**
   * @return  The message being sent
   */
  message(): Message;

  /**
   * Call the next interceptor
   */
  next(): void;

  /**
   */
  send(): boolean;
}

declare var SendContext: {
}
