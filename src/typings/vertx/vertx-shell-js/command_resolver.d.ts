/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./command.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-shell-js/command_resolver" {
  export = CommandResolver;
}

/**
 * A resolver for commands, so the shell can discover commands.
 */
interface CommandResolver
{

  /**
   * @return the current commands
   */
  commands(): Array<Command>;

  /**
   * Returns a single command by its name.
   */
  getCommand(name: string): Command;
}

declare var CommandResolver: {

  /**
   * @return the base commands of Vert.x Shell.
   */
  baseCommands(vertx: Vertx): CommandResolver;
}
