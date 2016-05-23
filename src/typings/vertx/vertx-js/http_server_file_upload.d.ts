/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./read_stream.d.ts" />

declare module "vertx-js/http_server_file_upload" {
  export = HttpServerFileUpload;
}

/**
 * Represents an file upload from an HTML FORM.
 */
interface HttpServerFileUpload
  extends
    ReadStream
{
  exceptionHandler(handler: (e: Throwable) => void): HttpServerFileUpload;
  handler(handler: (e: Buffer) => void): HttpServerFileUpload;
  endHandler(endHandler: (e: void) => void): HttpServerFileUpload;
  pause(): HttpServerFileUpload;
  resume(): HttpServerFileUpload;

  /**
   * Stream the content of this upload to the given file on storage.
   */
  streamToFileSystem(filename: string): HttpServerFileUpload;

  /**
   * @return the filename which was used when upload the file.
   */
  filename(): string;

  /**
   * @return the name of the attribute
   */
  name(): string;

  /**
   * @return  the content type for the upload
   */
  contentType(): string;

  /**
   * @return the contentTransferEncoding for the upload
   */
  contentTransferEncoding(): string;

  /**
   * @return the charset for the upload
   */
  charset(): string;

  /**
   * The size of the upload may not be available until it is all read.
   * Check isSizeAvailable to determine this
   */
  size(): number;

  /**
   * @return true if the size of the upload can be retrieved via size.
   */
  isSizeAvailable(): boolean;
}

declare var HttpServerFileUpload: {
}
