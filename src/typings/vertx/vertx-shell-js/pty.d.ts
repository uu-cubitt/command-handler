/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./tty.d.ts" />

declare module "vertx-shell-js/pty" {
  export = Pty;
}

/**
 * A pseudo terminal used for controlling a Tty. This interface acts as a pseudo
 * terminal master, slave returns the assocated slave pseudo terminal.
 */
interface Pty
{

  /**
   * Set the standard out handler of the pseudo terminal.
   */
  stdoutHandler(handler: (e: string) => void): Pty;

  /**
   * Write data to the slave standard input of the pseudo terminal.
   */
  write(data: string): Pty;

  /**
   * Resize the terminal.
   */
  setSize(width: number, height: number): Pty;

  /**
   * @return the pseudo terminal slave
   */
  slave(): Tty;
}

declare var Pty: {

  /**
   * Create a new pseudo terminal with no terminal type.
   */
  create(): Pty;

  /**
   * Create a new pseudo terminal.
   */
  create(terminalType: string): Pty;
}
