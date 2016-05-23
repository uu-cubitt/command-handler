/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-jdbc-js/jdbc_client.d.ts" />
/// <reference path="../vertx-auth-common-js/auth_provider.d.ts" />

declare module "vertx-auth-jdbc-js/jdbc_auth" {
  export = JDBCAuth;
}

/**
 * Factory interface for creating AuthProvider instances that use the Vert.x JDBC client
 */
interface JDBCAuth
  extends AuthProvider
{

  /**
   * Set the authentication query to use. Use this if you want to override the default authentication query.
   */
  setAuthenticationQuery(authenticationQuery: string): JDBCAuth;

  /**
   * Set the roles query to use. Use this if you want to override the default roles query.
   */
  setRolesQuery(rolesQuery: string): JDBCAuth;

  /**
   * Set the permissions query to use. Use this if you want to override the default permissions query.
   */
  setPermissionsQuery(permissionsQuery: string): JDBCAuth;

  /**
   * Set the role prefix to distinguish from permissions when checking for isPermitted requests.
   */
  setRolePrefix(rolePrefix: string): JDBCAuth;
}

declare var JDBCAuth: {

  /**
   * Create a JDBC auth provider implementation
   */
  create(client: JDBCClient): JDBCAuth;
}
