/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./template_engine.d.ts" />

declare module "vertx-web-js/handlebars_template_engine" {
  export = HandlebarsTemplateEngine;
}

/**
 * A template engine that uses the Handlebars library.
 */
interface HandlebarsTemplateEngine
  extends TemplateEngine
{

  /**
   * Set the extension for the engine
   */
  setExtension(extension: string): HandlebarsTemplateEngine;

  /**
   * Set the max cache size for the engine
   */
  setMaxCacheSize(maxCacheSize: number): HandlebarsTemplateEngine;
}

declare var HandlebarsTemplateEngine: {

  /**
   * Create a template engine using defaults
   */
  create(): HandlebarsTemplateEngine;
}
