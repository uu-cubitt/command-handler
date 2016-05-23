/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-web-js/locale" {
  export = Locale;
}
interface Locale
{

  /**
   * Returns the language as reported by the HTTP client.
   */
  language(): string;

  /**
   * Returns the country as reported by the HTTP client.
   */
  country(): string;

  /**
   * Returns the variant as reported by the HTTP client.
   */
  variant(): string;
}

declare var Locale: {
  create(): Locale;
  create(language: string): Locale;
  create(language: string, country: string): Locale;
  create(language: string, country: string, variant: string): Locale;
}
