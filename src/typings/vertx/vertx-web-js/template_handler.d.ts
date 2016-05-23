/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />
/// <reference path="./template_engine.d.ts" />
/// <reference path="./routing_context.d.ts" />

declare module "vertx-web-js/template_handler" {
  export = TemplateHandler;
}

/**
 *
 * A handler which renders responses using a template engine and where the template name is selected from the URI
 * path.
 */
interface TemplateHandler
{

  /**
   * Something has happened, so handle it.
   */
  handle(event: RoutingContext): void;
}

declare var TemplateHandler: {

  /**
   * Create a handler
   */
  create(engine: TemplateEngine): TemplateHandler;

  /**
   * Create a handler
   */
  create(engine: TemplateEngine, templateDirectory: string, contentType: string): TemplateHandler;
}
