/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-shell-js/tty" {
  export = Tty;
}

/**
 * Provide interactions with the Shell TTY.
 */
interface Tty
{

  /**
   * @return the declared tty type, for instance , ,  etc... it can be null
   * when the tty does not have declared its type.
   */
  type(): string;

  /**
   * @return the current width, i.e the number of rows or  if unknown
   */
  width(): number;

  /**
   * @return the current height, i.e the number of columns or  if unknown
   */
  height(): number;

  /**
   * Set a stream handler on the standard input to read the data.
   */
  stdinHandler(handler: (e: string) => void): Tty;

  /**
   * Write data to the standard output.
   */
  write(data: string): Tty;

  /**
   * Set a resize handler, the handler is called when the tty size changes.
   */
  resizehandler(handler: (e: void) => void): Tty;
}

declare var Tty: {
}
