/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./completion.d.ts" />
/// <reference path="./signal_handler.d.ts" />
/// <reference path="./tty.d.ts" />
/// <reference path="./session.d.ts" />

declare module "vertx-shell-js/term" {
  export = Term;
}

/**
 * The terminal.
 */
interface Term
  extends Tty
{
  resizehandler(handler: (e: void) => void): Term;
  stdinHandler(handler: (e: string) => void): Term;
  write(data: string): Term;

  /**
   * @return the last time this term received input
   */
  lastAccessedTime(): number;

  /**
   * Echo some text in the terminal, escaped if necessary.<p/>
   */
  echo(text: string): Term;

  /**
   * Associate the term with a session.
   */
  setSession(session: ShellSession): Term;

  /**
   * Set an interrupt signal handler on the term.
   */
  interruptHandler(handler: SignalHandler): Term;

  /**
   * Set a suspend signal handler on the term.
   */
  suspendHandler(handler: SignalHandler): Term;

  /**
   * Prompt the user a line of text.
   */
  readline(prompt: string, lineHandler: (e: string) => void): void;

  /**
   * Prompt the user a line of text, providing a completion handler to handle user's completion.
   */
  readline(prompt: string, lineHandler: (e: string) => void, completionHandler: (e: Completion) => void): void;

  /**
   * Set a handler that will be called when the terminal is closed.
   */
  closeHandler(handler: (e: void) => void): Term;

  /**
   * Close the connection to terminal.
   */
  close(): void;
}

declare var Term: {
}
