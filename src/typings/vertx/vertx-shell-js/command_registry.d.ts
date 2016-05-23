/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./command.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./command_resolver.d.ts" />

declare module "vertx-shell-js/command_registry" {
  export = CommandRegistry;
}

/**
 * A registry that contains the commands known by a shell.<p/>
 *
 * It is a mutable command resolver.
 */
interface CommandRegistry
  extends CommandResolver
{

  /**
   * Like registerCommand, without a completion handler.
   */
  registerCommand(command: Command): CommandRegistry;

  /**
   * Register a command
   */
  registerCommand(command: Command, completionHandler: (res: Command, err?: Throwable) => void): CommandRegistry;

  /**
   * Like registerCommands, without a completion handler.
   */
  registerCommands(commands: Array<Command>): CommandRegistry;

  /**
   * Register a list of commands.
   */
  registerCommands(commands: Array<Command>, completionHandler: (res: Array<Command>, err?: Throwable) => void): CommandRegistry;

  /**
   * Like unregisterCommand, without a completion handler.
   */
  unregisterCommand(commandName: string): CommandRegistry;

  /**
   * Unregister a command.
   */
  unregisterCommand(commandName: string, completionHandler: (res: void, err?: Throwable) => void): CommandRegistry;
}

declare var CommandRegistry: {

  /**
   * Get the shared registry for the Vert.x instance.
   */
  getShared(vertx: Vertx): CommandRegistry;

  /**
   * Create a new registry.
   */
  create(vertx: Vertx): CommandRegistry;
}
