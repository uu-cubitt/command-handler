/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./auth_provider.d.ts" />

declare module "vertx-auth-common-js/user" {
  export = User;
}

/**
 * Represents an authenticates User and contains operations to authorise the user.
 * <p>
 * Please consult the documentation for a detailed explanation.
 */
interface User
{

  /**
   * Is the user authorised to
   */
  isAuthorised(authority: string, resultHandler: (res: boolean, err?: Throwable) => void): User;

  /**
   * The User object will cache any authorities that it knows it has to avoid hitting the
   * underlying auth provider each time.  Use this method if you want to clear this cache.
   */
  clearCache(): User;

  /**
   * Get the underlying principal for the User. What this actually returns depends on the implementation.
   * For a simple user/password based auth, it's likely to contain a JSON object with the following structure:
   * <pre>
   *   {
   *     "username", "tim"
   *   }
   * </pre>
   */
  principal(): any;

  /**
   * Set the auth provider for the User. This is typically used to reattach a detached User with an AuthProvider, e.g.
   * after it has been deserialized.
   */
  setAuthProvider(authProvider: AuthProvider): void;
}

declare var User: {
}
