/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />
/// <reference path="../vertx-auth-common-js/auth_provider.d.ts" />

declare module "vertx-web-js/user_session_handler" {
  export = UserSessionHandler;
}

/**
 *
 * This handler should be used if you want to store the User object in the Session so it's available between
 * different requests, without you having re-authenticate each time.
 *
 * It requires that the session handler is already present on previous matching routes.
 *
 * It requires an Auth provider so, if the user is deserialized from a clustered session it knows which Auth provider
 * to associate the session with.
 */
interface UserSessionHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var UserSessionHandler: {

  /**
   * Create a new handler
   */
  create(authProvider: AuthProvider): UserSessionHandler;
}
