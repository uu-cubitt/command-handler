/// <reference path="./throwable.d.ts" />
/// <reference path="./command_line.d.ts" />

declare module "vertx-js/cli" {
  export = CLI;
}

/**
 * Interface defining a command-line interface (in other words a command such as 'run', 'ls'...).
 * This interface is polyglot to ease reuse such as in Vert.x Shell.
 * <p/>
 * A command line interface has a name, and defines a set of options and arguments. Options are key-value pair such
 * as <code>-foo=bar</code> or <code>-flag</code>. The supported formats depend on the used parser. Arguments are unlike
 * options raw values. Options are defined using
 * Option, while argument are defined using Argument.
 * <p/>
 * Command line interfaces also define a summary and a description. These attributes are used in the usage generation
 * . To disable the help generation, set the <code>hidden</code> attribute to <code>true</code>.
 * <p/>
 * Command Line Interface object does not contains "value", it's a model. It must be evaluated by a
 * parser that returns a CommandLine object containing the argument and option values.
 */
interface CLI
{

  /**
   * Parses the user command line interface and create a new CommandLine containing extracting values.
   */
  parse(arguments: Array<string>): CommandLine;

  /**
   * Parses the user command line interface and create a new CommandLine containing extracting values.
   */
  parse(arguments: Array<string>, validate: boolean): CommandLine;

  /**
   * @return the CLI name.
   */
  getName(): string;

  /**
   * Sets the name of the CLI.
   */
  setName(name: string): CLI;

  /**
   * @return the CLI description.
   */
  getDescription(): string;
  setDescription(desc: string): CLI;

  /**
   * @return the CLI summary.
   */
  getSummary(): string;

  /**
   * Sets the summary of the CLI.
   */
  setSummary(summary: string): CLI;

  /**
   * Checks whether or not the current CLI instance is hidden.
   */
  isHidden(): boolean;

  /**
   * Sets whether or not the current instance of CLI must be hidden. Hidden CLI are not listed when
   * displaying usages / help messages. In other words, hidden commands are for power user.
   */
  setHidden(hidden: boolean): CLI;

  /**
   * Gets the list of options.
   */
  getOptions(): Array<any>;

  /**
   * Adds an option.
   */
  addOption(option: any): CLI;

  /**
   * Adds a set of options. Unlike setOptions}, this method does not remove the existing options.
   * The given list is appended to the existing list.
   */
  addOptions(options: Array<any>): CLI;

  /**
   * Sets the list of arguments.
   */
  setOptions(options: Array<any>): CLI;

  /**
   * Gets the list of defined arguments.
   */
  getArguments(): Array<any>;

  /**
   * Adds an argument.
   */
  addArgument(arg: any): CLI;

  /**
   * Adds a set of arguments. Unlike setArguments, this method does not remove the existing arguments.
   * The given list is appended to the existing list.
   */
  addArguments(args: Array<any>): CLI;

  /**
   * Sets the list of arguments.
   */
  setArguments(args: Array<any>): CLI;

  /**
   * Gets an Option based on its name (short name, long name or argument name).
   */
  getOption(name: string): any;

  /**
   * Gets an Argument based on its name (argument name).
   */
  getArgument(name: string): any;

  /**
   * Gets an Argument based on its index.
   */
  getArgument(index: number): any;

  /**
   * Removes an option identified by its name. This method does nothing if the option cannot be found.
   */
  removeOption(name: string): CLI;

  /**
   * Removes an argument identified by its index. This method does nothing if the argument cannot be found.
   */
  removeArgument(index: number): CLI;
}

declare var CLI: {

  /**
   * Creates an instance of CLI using the default implementation.
   */
  create(name: string): CLI;
}
