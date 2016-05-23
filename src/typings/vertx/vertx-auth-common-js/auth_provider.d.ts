/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./user.d.ts" />

declare module "vertx-auth-common-js/auth_provider" {
  export = AuthProvider;
}

/**
 *
 * User-facing interface for authenticating users.
 */
interface AuthProvider
{

  /**
   * Authenticate a user.
   * <p>
   * The first argument is a JSON object containing information for authenticating the user. What this actually contains
   * depends on the specific implementation. In the case of a simple username/password based
   * authentication it is likely to contain a JSON object with the following structure:
   * <pre>
   *   {
   *     "username": "tim",
   *     "password": "mypassword"
   *   }
   * </pre>
   * For other types of authentication it contain different information - for example a JWT token or OAuth bearer token.
   * <p>
   * If the user is successfully authenticated a User object is passed to the handler in an AsyncResult.
   * The user object can then be used for authorisation.
   */
  authenticate(authInfo: any, resultHandler: (res: User, err?: Throwable) => void): void;
}

declare var AuthProvider: {
}
