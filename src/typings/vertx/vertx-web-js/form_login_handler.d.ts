/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />
/// <reference path="../vertx-auth-common-js/auth_provider.d.ts" />

declare module "vertx-web-js/form_login_handler" {
  export = FormLoginHandler;
}

/**
 * Handler that handles login from a form on a custom login page.
 * <p>
 * Used in conjunction with the RedirectAuthHandler.
 */
interface FormLoginHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;

  /**
   * Set the name of the form param used to submit the username
   */
  setUsernameParam(usernameParam: string): FormLoginHandler;

  /**
   * Set the name of the form param used to submit the password
   */
  setPasswordParam(passwordParam: string): FormLoginHandler;

  /**
   * Set the name of the session attrioute used to specify the return url
   */
  setReturnURLParam(returnURLParam: string): FormLoginHandler;

  /**
   * Set the url to redirect to if the user logs in directly at the url of the form login handler
   * without being redirected here first
   */
  setDirectLoggedInOKURL(directLoggedInOKURL: string): FormLoginHandler;
}

declare var FormLoginHandler: {

  /**
   * Create a handler
   */
  create(authProvider: AuthProvider): FormLoginHandler;

  /**
   * Create a handler
   */
  create(authProvider: AuthProvider, usernameParam: string, passwordParam: string, returnURLParam: string, directLoggedInOKURL: string): FormLoginHandler;
}
