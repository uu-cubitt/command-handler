/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./command.d.ts" />
/// <reference path="./completion.d.ts" />
/// <reference path="../vertx-js/cli.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./command_process.d.ts" />

declare module "vertx-shell-js/command_builder" {
  export = CommandBuilder;
}

/**
 * A build for Vert.x Shell command.
 */
interface CommandBuilder
{

  /**
   * Set the command process handler, the process handler is called when the command is executed.
   */
  processHandler(handler: (e: CommandProcess) => void): CommandBuilder;

  /**
   * Set the command completion handler, the completion handler when the user asks for contextual command line
   * completion, usually hitting the <i>tab</i> key.
   */
  completionHandler(handler: (e: Completion) => void): CommandBuilder;

  /**
   * Build the command
   */
  build(vertx: Vertx): Command;
}

declare var CommandBuilder: {

  /**
   * Create a new commmand builder, the command is responsible for managing the options and arguments via the
   * #args() arguments.
   */
  command(name: string): CommandBuilder;

  /**
   * Create a new commmand with its CLI descriptor. This command can then retrieve the parsed
   * commandLine when it executes to know get the command arguments and options.
   */
  command(cli: CLI): CommandBuilder;
}
