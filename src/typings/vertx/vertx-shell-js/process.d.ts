/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./tty.d.ts" />
/// <reference path="./session.d.ts" />

declare module "vertx-shell-js/process" {
  export = Process;
}

/**
 * A process managed by the shell.
 */
interface Process
{

  /**
   * @return the current process status
   */
  status(): any;

  /**
   * @return the process exit code when the status is  otherwise <code>null</code>
   */
  exitCode(): number;

  /**
   * Set the process tty.
   */
  setTty(tty: Tty): Process;

  /**
   * @return the process tty
   */
  getTty(): Tty;

  /**
   * Set the process session
   */
  setSession(session: ShellSession): Process;

  /**
   * @return the process session
   */
  getSession(): ShellSession;

  /**
   * Set an handler for being notified when the process terminates.
   */
  terminatedHandler(handler: (e: number) => void): Process;

  /**
   * Run the process.
   */
  run(): void;

  /**
   * Run the process.
   */
  run(foreground: boolean): void;

  /**
   * Run the process.
   */
  run(completionHandler: (e: void) => void): void;

  /**
   * Run the process.
   */
  run(foregraound: boolean, completionHandler: (e: void) => void): void;

  /**
   * Attempt to interrupt the process.
   */
  interrupt(): boolean;

  /**
   * Attempt to interrupt the process.
   */
  interrupt(completionHandler: (e: void) => void): boolean;

  /**
   * Suspend the process.
   */
  resume(): void;

  /**
   * Suspend the process.
   */
  resume(foreground: boolean): void;

  /**
   * Suspend the process.
   */
  resume(completionHandler: (e: void) => void): void;

  /**
   * Suspend the process.
   */
  resume(foreground: boolean, completionHandler: (e: void) => void): void;

  /**
   * Resume the process.
   */
  suspend(): void;

  /**
   * Resume the process.
   */
  suspend(completionHandler: (e: void) => void): void;

  /**
   * Terminate the process.
   */
  terminate(): void;

  /**
   * Terminate the process.
   */
  terminate(completionHandler: (e: void) => void): void;

  /**
   * Set the process in background.
   */
  toBackground(): void;

  /**
   * Set the process in background.
   */
  toBackground(completionHandler: (e: void) => void): void;

  /**
   * Set the process in foreground.
   */
  toForeground(): void;

  /**
   * Set the process in foreground.
   */
  toForeground(completionHandler: (e: void) => void): void;
}

declare var Process: {
}
