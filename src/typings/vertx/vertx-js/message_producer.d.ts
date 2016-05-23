/// <reference path="./throwable.d.ts" />
/// <reference path="./write_stream.d.ts" />
/// <reference path="./message.d.ts" />

declare module "vertx-js/message_producer" {
  export = MessageProducer;
}

/**
 * Represents a stream of message that can be written to.
 * <p>
 */
interface MessageProducer
  extends
    WriteStream
{

  /**
   * Same as end but writes some data to the stream before ending.
   */
  end(t: any): void;

  /**
   * This will return <code>true</code> if there are more bytes in the write queue than the value set using setWriteQueueMaxSize
   */
  writeQueueFull(): boolean;

  /**
   * Synonym for write.
   */
  send(message: any): MessageProducer;
  send(message: any, replyHandler: (res: Message, err?: Throwable) => void): MessageProducer;
  exceptionHandler(handler: (e: Throwable) => void): MessageProducer;
  write(data: any): MessageProducer;
  setWriteQueueMaxSize(maxSize: number): MessageProducer;
  drainHandler(handler: (e: void) => void): MessageProducer;

  /**
   * Update the delivery options of this producer.
   */
  deliveryOptions(options: any): MessageProducer;

  /**
   * @return The address to which the producer produces messages.
   */
  address(): string;

  /**
   * Closes the producer, calls close
   */
  end(): void;

  /**
   * Closes the producer, this method should be called when the message producer is not used anymore.
   */
  close(): void;
}

declare var MessageProducer: {
}
