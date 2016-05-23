/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./process.d.ts" />
/// <reference path="./tty.d.ts" />
/// <reference path="./session.d.ts" />

declare module "vertx-shell-js/job" {
  export = Job;
}

/**
 * A job executed in a JobController, grouping one or several process.<p/>
 *
 * The job life cycle can be controlled with the run, resume and suspend and interrupt
 * methods.
 */
interface Job
{

  /**
   * @return the job id
   */
  id(): number;

  /**
   * @return the job exec status
   */
  status(): any;

  /**
   * @return when the job was last stopped
   */
  lastStopped(): number;

  /**
   * @return the execution line of the job, i.e the shell command line that launched this job
   */
  line(): string;

  /**
   * Set a tty on the job.
   */
  setTty(tty: Tty): Job;

  /**
   * Set a session on the job.
   */
  setSession(session: ShellSession): Job;

  /**
   * Set an handler called when the job terminates.
   */
  statusUpdateHandler(handler: (e: any) => void): Job;

  /**
   * Run the job, before running the job a  must be set.
   */
  run(): Job;

  /**
   * Attempt to interrupt the job.
   */
  interrupt(): boolean;

  /**
   * Resume the job to foreground.
   */
  resume(): Job;

  /**
   * Send the job to background.
   */
  toBackground(): Job;

  /**
   * Send the job to foreground.
   */
  toForeground(): Job;

  /**
   * Resume the job.
   */
  resume(foreground: boolean): Job;

  /**
   * Resume the job.
   */
  suspend(): Job;

  /**
   * Terminate the job.
   */
  terminate(): void;

  /**
   * @return the first process in the job
   */
  process(): Process;
}

declare var Job: {
}
