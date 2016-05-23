/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-auth-common-js/user.d.ts" />

declare module "vertx-auth-oauth2-js/access_token" {
  export = AccessToken;
}

/**
 * AccessToken extension to the User interface
 */
interface AccessToken
  extends User
{

  /**
   * Check if the access token is expired or not.
   */
  expired(): boolean;

  /**
   * Refresh the access token
   */
  refresh(callback: (res: void, err?: Throwable) => void): AccessToken;

  /**
   * Revoke access or refresh token
   */
  revoke(token_type: string, callback: (res: void, err?: Throwable) => void): AccessToken;
}

declare var AccessToken: {
}
