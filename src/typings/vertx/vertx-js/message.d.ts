/// <reference path="./throwable.d.ts" />
/// <reference path="./multi_map.d.ts" />

declare module "vertx-js/message" {
  export = Message;
}

/**
 * Represents a message that is received from the event bus in a handler.
 * <p>
 * Messages have a body, which can be null, and also headers, which can be empty.
 * <p>
 * If the message was sent specifying a reply handler it will also have a replyAddress. In that case the message
 * can be replied to using that reply address, or, more simply by just using reply.
 * <p>
 * If you want to notify the sender that processing failed, then fail can be called.
 */
interface Message
{

  /**
   * The address the message was sent to
   */
  address(): string;

  /**
   * Multi-map of message headers. Can be empty
   */
  headers(): MultiMap;

  /**
   * The body of the message. Can be null.
   */
  body(): any;

  /**
   * The reply address. Can be null.
   */
  replyAddress(): string;

  /**
   * Reply to this message.
   * <p>
   * If the message was sent specifying a reply handler, that handler will be
   * called when it has received a reply. If the message wasn't sent specifying a receipt handler
   * this method does nothing.
   */
  reply(message: any): void;

  /**
   * The same as <code>reply(R message)</code> but you can specify handler for the reply - i.e.
   * to receive the reply to the reply.
   */
  reply(message: any, replyHandler: (res: Message, err?: Throwable) => void): void;

  /**
   * Link reply but allows you to specify delivery options for the reply.
   */
  reply(message: any, options: any): void;

  /**
   * The same as <code>reply(R message, DeliveryOptions)</code> but you can specify handler for the reply - i.e.
   * to receive the reply to the reply.
   */
  reply(message: any, options: any, replyHandler: (res: Message, err?: Throwable) => void): void;

  /**
   * Signal to the sender that processing of this message failed.
   * <p>
   * If the message was sent specifying a result handler
   * the handler will be called with a failure corresponding to the failure code and message specified here.
   */
  fail(failureCode: number, message: string): void;
}

declare var Message: {
}
