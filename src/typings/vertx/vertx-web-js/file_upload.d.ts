/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-web-js/file_upload" {
  export = FileUpload;
}

/**
 * Represents a file-upload from an HTTP multipart form submission.
 * <p>
 */
interface FileUpload
{

  /**
   * @return the name of the upload as provided in the form submission
   */
  name(): string;

  /**
   * @return the actual temporary file name on the server where the file was uploaded to.
   */
  uploadedFileName(): string;

  /**
   * @return the file name of the upload as provided in the form submission
   */
  fileName(): string;

  /**
   * @return the size of the upload, in bytes
   */
  size(): number;

  /**
   * @return the content type (MIME type) of the upload
   */
  contentType(): string;

  /**
   * @return the content transfer encoding of the upload - this describes how the upload was encoded in the form submission.
   */
  contentTransferEncoding(): string;

  /**
   * @return the charset of the upload
   */
  charSet(): string;
}

declare var FileUpload: {
}
