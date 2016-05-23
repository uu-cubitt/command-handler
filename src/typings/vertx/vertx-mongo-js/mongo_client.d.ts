/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-mongo-js/mongo_client" {
  export = MongoClient;
}

/**
 * A Vert.x service used to interact with MongoDB server instances.
 */
interface MongoClient
{

  /**
   * Save a document in the specified collection
   */
  save(collection: string, document: any, resultHandler: (res: string, err?: Throwable) => void): MongoClient;

  /**
   * Save a document in the specified collection with the specified write option
   */
  saveWithOptions(collection: string, document: any, writeOption: any, resultHandler: (res: string, err?: Throwable) => void): MongoClient;

  /**
   * Insert a document in the specified collection
   */
  insert(collection: string, document: any, resultHandler: (res: string, err?: Throwable) => void): MongoClient;

  /**
   * Insert a document in the specified collection with the specified write option
   */
  insertWithOptions(collection: string, document: any, writeOption: any, resultHandler: (res: string, err?: Throwable) => void): MongoClient;

  /**
   * Update matching documents in the specified collection
   */
  update(collection: string, query: any, update: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Update matching documents in the specified collection, specifying options
   */
  updateWithOptions(collection: string, query: any, update: any, options: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Replace matching documents in the specified collection
   */
  replace(collection: string, query: any, replace: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Replace matching documents in the specified collection, specifying options
   */
  replaceWithOptions(collection: string, query: any, replace: any, options: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Find matching documents in the specified collection
   */
  find(collection: string, query: any, resultHandler: (res: Array<any>, err?: Throwable) => void): MongoClient;

  /**
   * Find matching documents in the specified collection.
   * This method use batchCursor for returning each found document.
   */
  findBatch(collection: string, query: any, resultHandler: (res: any, err?: Throwable) => void): MongoClient;

  /**
   * Find matching documents in the specified collection, specifying options
   */
  findWithOptions(collection: string, query: any, options: any, resultHandler: (res: Array<any>, err?: Throwable) => void): MongoClient;

  /**
   * Find matching documents in the specified collection, specifying options.
   * This method use batchCursor for returning each found document.
   */
  findBatchWithOptions(collection: string, query: any, options: any, resultHandler: (res: any, err?: Throwable) => void): MongoClient;

  /**
   * Find a single matching document in the specified collection
   */
  findOne(collection: string, query: any, fields: any, resultHandler: (res: any, err?: Throwable) => void): MongoClient;

  /**
   * Count matching documents in a collection.
   */
  count(collection: string, query: any, resultHandler: (res: number, err?: Throwable) => void): MongoClient;

  /**
   * Remove matching documents from a collection
   */
  remove(collection: string, query: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Remove matching documents from a collection with the specified write option
   */
  removeWithOptions(collection: string, query: any, writeOption: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Remove a single matching document from a collection
   */
  removeOne(collection: string, query: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Remove a single matching document from a collection with the specified write option
   */
  removeOneWithOptions(collection: string, query: any, writeOption: any, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Create a new collection
   */
  createCollection(collectionName: string, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Get a list of all collections in the database.
   */
  getCollections(resultHandler: (res: Array<string>, err?: Throwable) => void): MongoClient;

  /**
   * Drop a collection
   */
  dropCollection(collection: string, resultHandler: (res: void, err?: Throwable) => void): MongoClient;

  /**
   * Run an arbitrary MongoDB command.
   */
  runCommand(commandName: string, command: any, resultHandler: (res: any, err?: Throwable) => void): MongoClient;

  /**
   * Gets the distinct values of the specified field name.
   * Return a JsonArray containing distinct values (eg: [ 1 , 89 ])
   */
  distinct(collection: string, fieldName: string, resultClassname: string, resultHandler: (res: any[], err?: Throwable) => void): MongoClient;

  /**
   * Gets the distinct values of the specified field name.
   * This method use batchCursor for returning each found value.
   * Each value is a json fragment with fieldName key (eg: {"num": 1}).
   */
  distinctBatch(collection: string, fieldName: string, resultClassname: string, resultHandler: (res: any, err?: Throwable) => void): MongoClient;

  /**
   * Close the client and release its resources
   */
  close(): void;
}

declare var MongoClient: {

  /**
   * Create a Mongo client which maintains its own data source.
   */
  createNonShared(vertx: Vertx, config: any): MongoClient;

  /**
   * Create a Mongo client which shares its data source with any other Mongo clients created with the same
   * data source name
   */
  createShared(vertx: Vertx, config: any, dataSourceName: string): MongoClient;

  /**
   * Like createShared but with the default data source name
   */
  createShared(vertx: Vertx, config: any): MongoClient;
}
