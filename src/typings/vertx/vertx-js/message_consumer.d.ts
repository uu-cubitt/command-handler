/// <reference path="./throwable.d.ts" />
/// <reference path="./read_stream.d.ts" />
/// <reference path="./message.d.ts" />

declare module "vertx-js/message_consumer" {
  export = MessageConsumer;
}

/**
 * An event bus consumer object representing a stream of message to an EventBus address that can
 * be read from.
 * <p>
 * The consumer or localConsumer
 * creates a new consumer, the returned consumer is not yet registered against the event bus. Registration
 * is effective after the handler method is invoked.<p>
 *
 * The consumer is unregistered from the event bus using the unregister method or by calling the
 * handler with a null value..
 */
interface MessageConsumer
  extends
    ReadStream
{
  exceptionHandler(handler: (e: Throwable) => void): MessageConsumer;
  handler(handler: (e: Message) => void): MessageConsumer;
  pause(): MessageConsumer;
  resume(): MessageConsumer;
  endHandler(endHandler: (e: void) => void): MessageConsumer;

  /**
   * @return a read stream for the body of the message stream.
   */
  bodyStream(): ReadStream;

  /**
   * @return true if the current consumer is registered
   */
  isRegistered(): boolean;

  /**
   * @return The address the handler was registered with.
   */
  address(): string;

  /**
   * Set the number of messages this registration will buffer when this stream is paused. The default
   * value is <code>0</code>. When a new value is set, buffered messages may be discarded to reach
   * the new value.
   */
  setMaxBufferedMessages(maxBufferedMessages: number): MessageConsumer;

  /**
   * @return the maximum number of messages that can be buffered when this stream is paused
   */
  getMaxBufferedMessages(): number;

  /**
   * Optional method which can be called to indicate when the registration has been propagated across the cluster.
   */
  completionHandler(completionHandler: (res: void, err?: Throwable) => void): void;

  /**
   * Unregisters the handler which created this registration
   */
  unregister(): void;

  /**
   * Unregisters the handler which created this registration
   */
  unregister(completionHandler: (res: void, err?: Throwable) => void): void;
}

declare var MessageConsumer: {
}
