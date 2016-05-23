/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./template_engine.d.ts" />

declare module "vertx-web-js/jade_template_engine" {
  export = JadeTemplateEngine;
}

/**
 * A template engine that uses Jade.
 */
interface JadeTemplateEngine
  extends TemplateEngine
{

  /**
   * Set the extension for the engine
   */
  setExtension(extension: string): JadeTemplateEngine;

  /**
   * Set the max cache size for the engine
   */
  setMaxCacheSize(maxCacheSize: number): JadeTemplateEngine;
}

declare var JadeTemplateEngine: {

  /**
   * Create a template engine using defaults
   */
  create(): JadeTemplateEngine;
}
