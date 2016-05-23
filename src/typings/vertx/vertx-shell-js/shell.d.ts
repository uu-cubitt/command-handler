/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./job.d.ts" />
/// <reference path="./job_controller.d.ts" />
/// <reference path="./cli_token.d.ts" />
/// <reference path="./session.d.ts" />

declare module "vertx-shell-js/shell" {
  export = Shell;
}

/**
 * An interactive session between a consumer and a shell.
 */
interface Shell
{

  /**
   * Create a job, the created job should then be executed with the run method.
   */
  createJob(line: Array<CliToken>): Job;

  /**
   * See createJob
   */
  createJob(line: string): Job;

  /**
   * @return the shell's job controller
   */
  jobController(): JobController;

  /**
   * @return the current shell session
   */
  session(): ShellSession;

  /**
   * Close the shell.
   */
  close(): void;
}

declare var Shell: {
}
