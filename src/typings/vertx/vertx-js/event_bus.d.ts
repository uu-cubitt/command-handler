/// <reference path="./throwable.d.ts" />
/// <reference path="./measured.d.ts" />
/// <reference path="./send_context.d.ts" />
/// <reference path="./message.d.ts" />
/// <reference path="./message_consumer.d.ts" />
/// <reference path="./message_producer.d.ts" />

declare module "vertx-js/event_bus" {
  export = EventBus;
}

/**
 * A Vert.x event-bus is a light-weight distributed messaging system which allows different parts of your application,
 * or different applications and services to communicate with each in a loosely coupled way.
 * <p>
 * An event-bus supports publish-subscribe messaging, point-to-point messaging and request-response messaging.
 * <p>
 * Message delivery is best-effort and messages can be lost if failure of all or part of the event bus occurs.
 * <p>
 * Please refer to the documentation for more information on the event bus.
 */
interface EventBus
  extends
    Measured
{

  /**
   * Whether the metrics are enabled for this measured object
   */
  isMetricsEnabled(): boolean;

  /**
   * Sends a message.
   * <p>
   * The message will be delivered to at most one of the handlers registered to the address.
   */
  send(address: string, message: any): EventBus;

  /**
   * Like send but specifying a <code>replyHandler</code> that will be called if the recipient
   * subsequently replies to the message.
   */
  send(address: string, message: any, replyHandler: (res: Message, err?: Throwable) => void): EventBus;

  /**
   * Like send but specifying <code>options</code> that can be used to configure the delivery.
   */
  send(address: string, message: any, options: any): EventBus;

  /**
   * Like send but specifying a <code>replyHandler</code> that will be called if the recipient
   * subsequently replies to the message.
   */
  send(address: string, message: any, options: any, replyHandler: (res: Message, err?: Throwable) => void): EventBus;

  /**
   * Publish a message.<p>
   * The message will be delivered to all handlers registered to the address.
   */
  publish(address: string, message: any): EventBus;

  /**
   * Like publish but specifying <code>options</code> that can be used to configure the delivery.
   */
  publish(address: string, message: any, options: any): EventBus;

  /**
   * Create a message consumer against the specified address.
   * <p>
   * The returned consumer is not yet registered
   * at the address, registration will be effective when handler
   * is called.
   */
  consumer(address: string): MessageConsumer;

  /**
   * Create a consumer and register it against the specified address.
   */
  consumer(address: string, handler: (e: Message) => void): MessageConsumer;

  /**
   * Like consumer but the address won't be propagated across the cluster.
   */
  localConsumer(address: string): MessageConsumer;

  /**
   * Like consumer but the address won't be propagated across the cluster.
   */
  localConsumer(address: string, handler: (e: Message) => void): MessageConsumer;

  /**
   * Create a message sender against the specified address.
   * <p>
   * The returned sender will invoke the send
   * method when the stream write method is called with the sender
   * address and the provided data.
   */
  sender(address: string): MessageProducer;

  /**
   * Like sender but specifying delivery options that will be used for configuring the delivery of
   * the message.
   */
  sender(address: string, options: any): MessageProducer;

  /**
   * Create a message publisher against the specified address.
   * <p>
   * The returned publisher will invoke the publish
   * method when the stream write method is called with the publisher
   * address and the provided data.
   */
  publisher(address: string): MessageProducer;

  /**
   * Like publisher but specifying delivery options that will be used for configuring the delivery of
   * the message.
   */
  publisher(address: string, options: any): MessageProducer;

  /**
   * Add an interceptor that will be called whenever a message is sent from Vert.x
   */
  addInterceptor(interceptor: (e: SendContext) => void): EventBus;

  /**
   * Remove an interceptor
   */
  removeInterceptor(interceptor: (e: SendContext) => void): EventBus;
}

declare var EventBus: {
}
