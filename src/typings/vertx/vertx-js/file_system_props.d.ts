/// <reference path="./throwable.d.ts" />

declare module "vertx-js/file_system_props" {
  export = FileSystemProps;
}

/**
 * Represents properties of the file system.
 */
interface FileSystemProps
{

  /**
   * @return The total space on the file system, in bytes
   */
  totalSpace(): number;

  /**
   * @return The total un-allocated space on the file system, in bytes
   */
  unallocatedSpace(): number;

  /**
   * @return The total usable space on the file system, in bytes
   */
  usableSpace(): number;
}

declare var FileSystemProps: {
}
