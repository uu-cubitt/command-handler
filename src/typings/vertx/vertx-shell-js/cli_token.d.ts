/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-shell-js/cli_token" {
  export = CliToken;
}

/**
 * A parsed token in the command line interface.
 */
interface CliToken
{

  /**
   * @return the token value
   */
  value(): string;

  /**
   * @return the raw token value, that may contain unescaped chars, for instance 
   */
  raw(): string;

  /**
   * @return true when it's a text token
   */
  isText(): boolean;

  /**
   * @return true when it's a blank token
   */
  isBlank(): boolean;
}

declare var CliToken: {

  /**
   * Create a text token.
   */
  createText(text: string): CliToken;

  /**
   * Create a new blank token.
   */
  createBlank(blank: string): CliToken;

  /**
   * Tokenize the string argument and return a list of tokens.
   */
  tokenize(s: string): Array<CliToken>;
}
