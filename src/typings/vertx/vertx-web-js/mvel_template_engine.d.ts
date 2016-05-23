/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./template_engine.d.ts" />

declare module "vertx-web-js/mvel_template_engine" {
  export = MVELTemplateEngine;
}

/**
 * A template engine that uses the Handlebars library.
 */
interface MVELTemplateEngine
  extends TemplateEngine
{

  /**
   * Set the extension for the engine
   */
  setExtension(extension: string): MVELTemplateEngine;

  /**
   * Set the max cache size for the engine
   */
  setMaxCacheSize(maxCacheSize: number): MVELTemplateEngine;
}

declare var MVELTemplateEngine: {

  /**
   * Create a template engine using defaults
   */
  create(): MVELTemplateEngine;
}
