/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/auth_handler" {
  export = AuthHandler;
}

/**
 * Base interface for auth handlers.
 * <p>
 * An auth handler allows your application to provide authentication/authorisation support.
 * <p>
 * Auth handler requires a SessionHandler to be on the routing chain before it.
 */
interface AuthHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;

  /**
   * Add a required authority for this auth handler
   */
  addAuthority(authority: string): AuthHandler;

  /**
   * Add a set of required authorities for this auth handler
   */
  addAuthorities(authorities: Array<string>): AuthHandler;
}

declare var AuthHandler: {
}
