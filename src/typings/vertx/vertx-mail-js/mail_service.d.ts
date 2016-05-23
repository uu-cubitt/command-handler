/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./mail_client.d.ts" />

declare module "vertx-mail-js/mail_service" {
  export = MailService;
}

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
interface MailService
  extends MailClient
{
  sendMail(email: any, resultHandler: (res: any, err?: Throwable) => void): MailService;
  close(): void;
}

declare var MailService: {

  /**
   * create a proxy of  MailService that delegates to the mail service running somewhere else via the event bus
   */
  createEventBusProxy(vertx: Vertx, address: string): MailService;
}
