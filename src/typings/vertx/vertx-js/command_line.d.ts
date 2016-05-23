/// <reference path="./throwable.d.ts" />
/// <reference path="./cli.d.ts" />

declare module "vertx-js/command_line" {
  export = CommandLine;
}

/**
 * The parser transforms a CLI (a model) into an CommandLine. This CommandLine
 * has stored the argument and option values. Only  instance of parser should create
 * objects of this type.
 */
interface CommandLine
{

  /**
   * @return the model of this command line object.
   */
  cli(): CLI;

  /**
   * @return the ordered list of arguments. Arguments are command line arguments not matching an option.
   */
  allArguments(): Array<string>;

  /**
   * Gets the value of an option with the matching name (can be the long name, short name or arg name).
   */
  getOptionValue(name: string): any;

  /**
   * Gets the value of an argument with the matching name (arg name).
   */
  getArgumentValue(name: string): any;

  /**
   * Gets the value of an argument with the given index.
   */
  getArgumentValue(index: number): any;

  /**
   * Gets the value of an option marked as a flag.
   * <p/>
   * Calling this method an a non-flag option throws an IllegalStateException.
   */
  isFlagEnabled(name: string): boolean;

  /**
   * Checks whether or not the given option has been assigned in the command line.
   */
  isOptionAssigned(option: any): boolean;

  /**
   * Gets the raw values of the given option. Raw values are simple "String", not converted to the option type.
   */
  getRawValues(option: any): Array<string>;

  /**
   * Gets the raw values of the given option. Raw values are simple "String", not converted to the option type.
   */
  getRawValuesForOption(option: any): Array<string>;

  /**
   * Gets the raw values of the given argument. Raw values are simple "String", not converted to the argument type.
   */
  getRawValuesForArgument(argument: any): Array<string>;

  /**
   * Gets the raw value of the given option. Raw values are the values as given in the user command line.
   */
  getRawValueForOption(option: any): string;

  /**
   * Checks whether or not the given option accept more values.
   */
  acceptMoreValues(option: any): boolean;

  /**
   * Gets the raw value of the given argument. Raw values are the values as given in the user command line.
   */
  getRawValueForArgument(arg: any): string;

  /**
   * Checks whether or not the given argument has been assigned in the command line.
   */
  isArgumentAssigned(arg: any): boolean;

  /**
   * Checks whether or not the given option has been seen in the user command line.
   */
  isSeenInCommandLine(option: any): boolean;

  /**
   * Checks whether or not the command line is valid, i.e. all constraints from arguments and options have been
   * satisfied. This method is used when the parser validation is disabled.
   */
  isValid(): boolean;

  /**
   * Checks whether or not the user has passed a "help" option and is asking for help.
   */
  isAskingForHelp(): boolean;
}

declare var CommandLine: {

  /**
   * Creates a command line object from the CLI. This object is intended to be used by
   * the parser to set the argument and option values.
   */
  create(cli: CLI): CommandLine;
}
