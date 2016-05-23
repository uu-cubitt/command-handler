/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./template_engine.d.ts" />

declare module "vertx-web-js/thymeleaf_template_engine" {
  export = ThymeleafTemplateEngine;
}

/**
 * A template engine that uses the Thymeleaf library.
 */
interface ThymeleafTemplateEngine
  extends TemplateEngine
{

  /**
   * Set the mode for the engine
   */
  setMode(mode: string): ThymeleafTemplateEngine;
}

declare var ThymeleafTemplateEngine: {

  /**
   * Create a template engine using defaults
   */
  create(): ThymeleafTemplateEngine;
}
