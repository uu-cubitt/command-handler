/// <reference path="./throwable.d.ts" />

declare module "vertx-js/file_props" {
  export = FileProps;
}

/**
 * Represents properties of a file on the file system.
 * <p>
 */
interface FileProps
{

  /**
   * The date the file was created
   */
  creationTime(): number;

  /**
   * The date the file was last accessed
   */
  lastAccessTime(): number;

  /**
   * The date the file was last modified
   */
  lastModifiedTime(): number;

  /**
   * Is the file a directory?
   */
  isDirectory(): boolean;

  /**
   * Is the file some other type? (I.e. not a directory, regular file or symbolic link)
   */
  isOther(): boolean;

  /**
   * Is the file a regular file?
   */
  isRegularFile(): boolean;

  /**
   * Is the file a symbolic link?
   */
  isSymbolicLink(): boolean;

  /**
   * The size of the file, in bytes
   */
  size(): number;
}

declare var FileProps: {
}
