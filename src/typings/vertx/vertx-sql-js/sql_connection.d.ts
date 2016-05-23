/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-sql-js/sql_connection" {
  export = SQLConnection;
}

/**
 * Represents a connection to a SQL database
 */
interface SQLConnection
{

  /**
   * Sets the auto commit flag for this connection. True by default.
   */
  setAutoCommit(autoCommit: boolean, resultHandler: (res: void, err?: Throwable) => void): SQLConnection;

  /**
   * Executes the given SQL statement
   */
  execute(sql: string, resultHandler: (res: void, err?: Throwable) => void): SQLConnection;

  /**
   * Executes the given SQL <code>SELECT</code> statement which returns the results of the query.
   */
  query(sql: string, resultHandler: (res: any, err?: Throwable) => void): SQLConnection;

  /**
   * Executes the given SQL <code>SELECT</code> prepared statement which returns the results of the query.
   */
  queryWithParams(sql: string, params: any[], resultHandler: (res: any, err?: Throwable) => void): SQLConnection;

  /**
   * Executes the given SQL statement which may be an <code>INSERT</code>, <code>UPDATE</code>, or <code>DELETE</code>
   * statement.
   */
  update(sql: string, resultHandler: (res: any, err?: Throwable) => void): SQLConnection;

  /**
   * Executes the given prepared statement which may be an <code>INSERT</code>, <code>UPDATE</code>, or <code>DELETE</code>
   * statement with the given parameters
   */
  updateWithParams(sql: string, params: any[], resultHandler: (res: any, err?: Throwable) => void): SQLConnection;

  /**
   * Calls the given SQL <code>PROCEDURE</code> which returns the result from the procedure.
   */
  call(sql: string, resultHandler: (res: any, err?: Throwable) => void): SQLConnection;

  /**
   * Calls the given SQL <code>PROCEDURE</code> which returns the result from the procedure.
   */
  callWithParams(sql: string, params: any[], outputs: any[], resultHandler: (res: any, err?: Throwable) => void): SQLConnection;

  /**
   * Closes the connection. Important to always close the connection when you are done so it's returned to the pool.
   */
  close(handler: (res: void, err?: Throwable) => void): void;

  /**
   * Closes the connection. Important to always close the connection when you are done so it's returned to the pool.
   */
  close(): void;

  /**
   * Commits all changes made since the previous commit/rollback.
   */
  commit(handler: (res: void, err?: Throwable) => void): SQLConnection;

  /**
   * Rolls back all changes made since the previous commit/rollback.
   */
  rollback(handler: (res: void, err?: Throwable) => void): SQLConnection;
}

declare var SQLConnection: {
}
