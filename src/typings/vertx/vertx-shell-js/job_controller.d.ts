/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./job.d.ts" />
/// <reference path="./process.d.ts" />

declare module "vertx-shell-js/job_controller" {
  export = JobController;
}

/**
 * The job controller.<p/>
 */
interface JobController
{

  /**
   * @return the current foreground job
   */
  foregroundJob(): Job;

  /**
   * @return the active jobs
   */
  jobs(): Array<Job>;

  /**
   * Returns an active job in this session by its .
   */
  getJob(id: number): Job;

  /**
   * Create a job wrapping a process.
   */
  createJob(process: Process, line: string): Job;

  /**
   * Close the controller and terminate all the underlying jobs, a closed controller does not accept anymore jobs.
   */
  close(completionHandler: (e: void) => void): void;

  /**
   * Close the shell session and terminate all the underlying jobs.
   */
  close(): void;
}

declare var JobController: {
}
