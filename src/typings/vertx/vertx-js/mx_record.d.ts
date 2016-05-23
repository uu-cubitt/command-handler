/// <reference path="./throwable.d.ts" />

declare module "vertx-js/mx_record" {
  export = MxRecord;
}

/**
 * Represent a Mail-Exchange-Record (MX) which was resolved for a domain.
 */
interface MxRecord
{

  /**
   * The priority of the MX record.
   */
  priority(): number;

  /**
   * The name of the MX record
   */
  name(): string;
}

declare var MxRecord: {
}
