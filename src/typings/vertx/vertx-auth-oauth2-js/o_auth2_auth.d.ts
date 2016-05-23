/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./access_token.d.ts" />
/// <reference path="../vertx-auth-common-js/auth_provider.d.ts" />

declare module "vertx-auth-oauth2-js/o_auth2_auth" {
  export = OAuth2Auth;
}

/**
 * Factory interface for creating OAuth2 based AuthProvider instances.
 */
interface OAuth2Auth
  extends AuthProvider
{

  /**
   * Generate a redirect URL to the authN/Z backend. It only applies to auth_code flow.
   */
  authorizeURL(params: any): string;

  /**
   * Returns the Access Token object.
   */
  getToken(params: any, handler: (res: AccessToken, err?: Throwable) => void): void;

  /**
   * Call OAuth2 APIs.
   */
  api(method: any, path: string, params: any, handler: (res: any, err?: Throwable) => void): OAuth2Auth;
}

declare var OAuth2Auth: {

  /**
   * Create a OAuth2 auth provider
   */
  create(vertx: Vertx, flow: any, config: any): OAuth2Auth;

  /**
   * Create a OAuth2 auth provider
   */
  create(vertx: Vertx, flow: any): OAuth2Auth;
}
