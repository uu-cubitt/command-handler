/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-sql-js/sql_connection.d.ts" />

declare module "vertx-jdbc-js/jdbc_client" {
  export = JDBCClient;
}

/**
 * An asynchronous client interface for interacting with a JDBC compliant database
 */
interface JDBCClient
{

  /**
   * Returns a connection that can be used to perform SQL operations on. It's important to remember
   * to close the connection when you are done, so it is returned to the pool.
   */
  getConnection(handler: (res: SQLConnection, err?: Throwable) => void): JDBCClient;

  /**
   * Close the client
   */
  close(): void;
}

declare var JDBCClient: {

  /**
   * Create a JDBC client which maintains its own data source.
   */
  createNonShared(vertx: Vertx, config: any): JDBCClient;

  /**
   * Create a JDBC client which shares its data source with any other JDBC clients created with the same
   * data source name
   */
  createShared(vertx: Vertx, config: any, dataSourceName: string): JDBCClient;

  /**
   * Like createShared but with the default data source name
   */
  createShared(vertx: Vertx, config: any): JDBCClient;
}
