/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-mail-js/mail_client" {
  export = MailClient;
}

/**
 * SMTP mail client for Vert.x
 * <p>
 * A simple asynchronous API for sending mails from Vert.x applications
 */
interface MailClient
{

  /**
   * send a single mail via MailClient
   */
  sendMail(email: any, resultHandler: (res: any, err?: Throwable) => void): MailClient;

  /**
   * close the MailClient
   */
  close(): void;
}

declare var MailClient: {

  /**
   * create a non shared instance of the mail client
   */
  createNonShared(vertx: Vertx, config: any): MailClient;

  /**
   * Create a Mail client which shares its data source with any other Mongo clients created with the same
   * pool name
   */
  createShared(vertx: Vertx, config: any, poolName: string): MailClient;

  /**
   * Like createShared but with the default pool name
   */
  createShared(vertx: Vertx, config: any): MailClient;
}
