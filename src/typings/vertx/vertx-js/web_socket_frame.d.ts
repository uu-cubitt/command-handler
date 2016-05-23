/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />

declare module "vertx-js/web_socket_frame" {
  export = WebSocketFrame;
}

/**
 * A WebSocket frame that represents either text or binary data.
 * <p>
 * A WebSocket message is composed of one or more WebSocket frames.
 * <p>
 * If there is a just a single frame in the message then a single text or binary frame should be created with final = true.
 * <p>
 * If there are more than one frames in the message, then the first frame should be a text or binary frame with
 * final = false, followed by one or more continuation frames. The last continuation frame should have final = true.
 */
interface WebSocketFrame
{

  /**
   * @return true if it's a text frame
   */
  isText(): boolean;

  /**
   * @return true if it's a binary frame
   */
  isBinary(): boolean;

  /**
   * @return true if it's a continuation frame
   */
  isContinuation(): boolean;

  /**
   * @return the content of this frame as a UTF-8 string and returns the
   * converted string. Only use this for text frames.
   */
  textData(): string;

  /**
   * @return the data of the frame
   */
  binaryData(): Buffer;

  /**
   * @return true if this is the final frame.
   */
  isFinal(): boolean;
}

declare var WebSocketFrame: {

  /**
   * Create a binary WebSocket frame.
   */
  binaryFrame(data: Buffer, isFinal: boolean): WebSocketFrame;

  /**
   * Create a text WebSocket frame.
   */
  textFrame(str: string, isFinal: boolean): WebSocketFrame;

  /**
   * Create a continuation frame
   */
  continuationFrame(data: Buffer, isFinal: boolean): WebSocketFrame;
}
