/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/command_line.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./cli_token.d.ts" />
/// <reference path="./tty.d.ts" />
/// <reference path="./session.d.ts" />

declare module "vertx-shell-js/command_process" {
  export = CommandProcess;
}

/**
 * The command process provides interaction with the process of the command provided by Vert.x Shell.
 */
interface CommandProcess
  extends Tty
{

  /**
   * @return the current Vert.x instance
   */
  vertx(): Vertx;

  /**
   * @return the unparsed arguments tokens
   */
  argsTokens(): Array<CliToken>;

  /**
   * @return the actual string arguments of the command
   */
  args(): Array<string>;

  /**
   * @return the command line object or null
   */
  commandLine(): CommandLine;

  /**
   * @return the shell session
   */
  session(): ShellSession;

  /**
   * @return true if the command is running in foreground
   */
  isForeground(): boolean;
  stdinHandler(handler: (e: string) => void): CommandProcess;

  /**
   * Set an interrupt handler, this handler is called when the command is interrupted, for instance user
   * press <code>Ctrl-C</code>.
   */
  interruptHandler(handler: (e: void) => void): CommandProcess;

  /**
   * Set a suspend handler, this handler is called when the command is suspended, for instance user
   * press <code>Ctrl-Z</code>.
   */
  suspendHandler(handler: (e: void) => void): CommandProcess;

  /**
   * Set a resume handler, this handler is called when the command is resumed, for instance user
   * types <code>bg</code> or <code>fg</code> to resume the command.
   */
  resumeHandler(handler: (e: void) => void): CommandProcess;

  /**
   * Set an end handler, this handler is called when the command is ended, for instance the command is running
   * and the shell closes.
   */
  endHandler(handler: (e: void) => void): CommandProcess;

  /**
   * Write some text to the standard output.
   */
  write(data: string): CommandProcess;

  /**
   * Set a background handler, this handler is called when the command is running and put to background.
   */
  backgroundHandler(handler: (e: void) => void): CommandProcess;

  /**
   * Set a foreground handler, this handler is called when the command is running and put to foreground.
   */
  foregroundHandler(handler: (e: void) => void): CommandProcess;
  resizehandler(handler: (e: void) => void): CommandProcess;

  /**
   * End the process with the exit status 
   */
  end(): void;

  /**
   * End the process.
   */
  end(status: number): void;
}

declare var CommandProcess: {
}
