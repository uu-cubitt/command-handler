/// <reference path="./throwable.d.ts" />
/// <reference path="./async_file.d.ts" />
/// <reference path="./buffer.d.ts" />
/// <reference path="./file_system_props.d.ts" />
/// <reference path="./file_props.d.ts" />

declare module "vertx-js/file_system" {
  export = FileSystem;
}

/**
 * Contains a broad set of operations for manipulating files on the file system.
 * <p>
 * A (potential) blocking and non blocking version of each operation is provided.
 * <p>
 * The non blocking versions take a handler which is called when the operation completes or an error occurs.
 * <p>
 * The blocking versions are named <code>xxxBlocking</code> and return the results, or throw exceptions directly.
 * In many cases, depending on the operating system and file system some of the potentially blocking operations
 * can return quickly, which is why we provide them, but it's highly recommended that you test how long they take to
 * return in your particular application before using them on an event loop.
 * <p>
 * Please consult the documentation for more information on file system support.
 */
interface FileSystem
{

  /**
   * Copy a file from the path <code>from</code> to path <code>to</code>, asynchronously.
   * <p>
   * The copy will fail if the destination already exists.
   */
  copy(from: string, to: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of copy
   */
  copyBlocking(from: string, to: string): FileSystem;

  /**
   * Copy a file from the path <code>from</code> to path <code>to</code>, asynchronously.
   * <p>
   * If <code>recursive</code> is <code>true</code> and <code>from</code> represents a directory, then the directory and its contents
   * will be copied recursively to the destination <code>to</code>.
   * <p>
   * The copy will fail if the destination if the destination already exists.
   */
  copyRecursive(from: string, to: string, recursive: boolean, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of copyRecursive
   */
  copyRecursiveBlocking(from: string, to: string, recursive: boolean): FileSystem;

  /**
   * Move a file from the path <code>from</code> to path <code>to</code>, asynchronously.
   * <p>
   * The move will fail if the destination already exists.
   */
  move(from: string, to: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of move
   */
  moveBlocking(from: string, to: string): FileSystem;

  /**
   * Truncate the file represented by <code>path</code> to length <code>len</code> in bytes, asynchronously.
   * <p>
   * The operation will fail if the file does not exist or <code>len</code> is less than <code>zero</code>.
   */
  truncate(path: string, len: number, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of truncate
   */
  truncateBlocking(path: string, len: number): FileSystem;

  /**
   * Change the permissions on the file represented by <code>path</code> to <code>perms</code>, asynchronously.
   * <p>
   * The permission String takes the form rwxr-x--- as
   * specified <a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>.
   */
  chmod(path: string, perms: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of #chmod(String, String, Handler) 
   */
  chmodBlocking(path: string, perms: string): FileSystem;

  /**
   * Change the permissions on the file represented by <code>path</code> to <code>perms</code>, asynchronously.<p>
   * The permission String takes the form rwxr-x--- as
   * specified in {<a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>}.
   * <p>
   * If the file is directory then all contents will also have their permissions changed recursively. Any directory permissions will
   * be set to <code>dirPerms</code>, whilst any normal file permissions will be set to <code>perms</code>.
   */
  chmodRecursive(path: string, perms: string, dirPerms: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of chmodRecursive
   */
  chmodRecursiveBlocking(path: string, perms: string, dirPerms: string): FileSystem;

  /**
   * Change the ownership on the file represented by <code>path</code> to <code>user</code> and {code group}, asynchronously.
   */
  chown(path: string, user: string, group: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of 
 *
   */
  chownBlocking(path: string, user: string, group: string): FileSystem;

  /**
   * Obtain properties for the file represented by <code>path</code>, asynchronously.
   * <p>
   * If the file is a link, the link will be followed.
   */
  props(path: string, handler: (res: FileProps, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of props
   */
  propsBlocking(path: string): FileProps;

  /**
   * Obtain properties for the link represented by <code>path</code>, asynchronously.
   * <p>
   * The link will not be followed.
   */
  lprops(path: string, handler: (res: FileProps, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of lprops
   */
  lpropsBlocking(path: string): FileProps;

  /**
   * Create a hard link on the file system from <code>link</code> to <code>existing</code>, asynchronously.
   */
  link(link: string, existing: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of link
   */
  linkBlocking(link: string, existing: string): FileSystem;

  /**
   * Create a symbolic link on the file system from <code>link</code> to <code>existing</code>, asynchronously.
   */
  symlink(link: string, existing: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of link
   */
  symlinkBlocking(link: string, existing: string): FileSystem;

  /**
   * Unlinks the link on the file system represented by the path <code>link</code>, asynchronously.
   */
  unlink(link: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of unlink
   */
  unlinkBlocking(link: string): FileSystem;

  /**
   * Returns the path representing the file that the symbolic link specified by <code>link</code> points to, asynchronously.
   */
  readSymlink(link: string, handler: (res: string, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of readSymlink
   */
  readSymlinkBlocking(link: string): string;

  /**
   * Deletes the file represented by the specified <code>path</code>, asynchronously.
   */
  delete(path: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of delete
   */
  deleteBlocking(path: string): FileSystem;

  /**
   * Deletes the file represented by the specified <code>path</code>, asynchronously.
   * <p>
   * If the path represents a directory and <code>recursive = true</code> then the directory and its contents will be
   * deleted recursively.
   */
  deleteRecursive(path: string, recursive: boolean, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of deleteRecursive
   */
  deleteRecursiveBlocking(path: string, recursive: boolean): FileSystem;

  /**
   * Create the directory represented by <code>path</code>, asynchronously.
   * <p>
   * The operation will fail if the directory already exists.
   */
  mkdir(path: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of mkdir
   */
  mkdirBlocking(path: string): FileSystem;

  /**
   * Create the directory represented by <code>path</code>, asynchronously.
   * <p>
   * The new directory will be created with permissions as specified by <code>perms</code>.
   * <p>
   * The permission String takes the form rwxr-x--- as specified
   * in <a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>.
   * <p>
   * The operation will fail if the directory already exists.
   */
  mkdir(path: string, perms: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of mkdir
   */
  mkdirBlocking(path: string, perms: string): FileSystem;

  /**
   * Create the directory represented by <code>path</code> and any non existent parents, asynchronously.
   * <p>
   * The operation will fail if the directory already exists.
   */
  mkdirs(path: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of mkdirs
   */
  mkdirsBlocking(path: string): FileSystem;

  /**
   * Create the directory represented by <code>path</code> and any non existent parents, asynchronously.
   * <p>
   * The new directory will be created with permissions as specified by <code>perms</code>.
   * <p>
   * The permission String takes the form rwxr-x--- as specified
   * in <a href="http://download.oracle.com/javase/7/docs/api/java/nio/file/attribute/PosixFilePermissions.html">here</a>.
   * <p>
   * The operation will fail if the directory already exists.<p>
   */
  mkdirs(path: string, perms: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of mkdirs
   */
  mkdirsBlocking(path: string, perms: string): FileSystem;

  /**
   * Read the contents of the directory specified by <code>path</code>, asynchronously.
   * <p>
   * The result is an array of String representing the paths of the files inside the directory.
   */
  readDir(path: string, handler: (res: Array<string>, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of readDir
   */
  readDirBlocking(path: string): Array<string>;

  /**
   * Read the contents of the directory specified by <code>path</code>, asynchronously.
   * <p>
   * The parameter <code>filter</code> is a regular expression. If <code>filter</code> is specified then only the paths that
   * match  @{filter}will be returned.
   * <p>
   * The result is an array of String representing the paths of the files inside the directory.
   */
  readDir(path: string, filter: string, handler: (res: Array<string>, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of readDir
   */
  readDirBlocking(path: string, filter: string): Array<string>;

  /**
   * Reads the entire file as represented by the path <code>path</code> as a , asynchronously.
   * <p>
   * Do not user this method to read very large files or you risk running out of available RAM.
   */
  readFile(path: string, handler: (res: Buffer, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of readFile
   */
  readFileBlocking(path: string): Buffer;

  /**
   * Creates the file, and writes the specified <code>Buffer data</code> to the file represented by the path <code>path</code>,
   * asynchronously.
   */
  writeFile(path: string, data: Buffer, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of writeFile
   */
  writeFileBlocking(path: string, data: Buffer): FileSystem;

  /**
   * Open the file represented by <code>path</code>, asynchronously.
   * <p>
   * The file is opened for both reading and writing. If the file does not already exist it will be created.
   */
  open(path: string, options: any, handler: (res: AsyncFile, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of open
   */
  openBlocking(path: string, options: any): AsyncFile;

  /**
   * Creates an empty file with the specified <code>path</code>, asynchronously.
   */
  createFile(path: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of createFile
   */
  createFileBlocking(path: string): FileSystem;

  /**
   * Creates an empty file with the specified <code>path</code> and permissions <code>perms</code>, asynchronously.
   */
  createFile(path: string, perms: string, handler: (res: void, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of createFile
   */
  createFileBlocking(path: string, perms: string): FileSystem;

  /**
   * Determines whether the file as specified by the path <code>path</code> exists, asynchronously.
   */
  exists(path: string, handler: (res: boolean, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of exists
   */
  existsBlocking(path: string): boolean;

  /**
   * Returns properties of the file-system being used by the specified <code>path</code>, asynchronously.
   */
  fsProps(path: string, handler: (res: FileSystemProps, err?: Throwable) => void): FileSystem;

  /**
   * Blocking version of fsProps
   */
  fsPropsBlocking(path: string): FileSystemProps;
}

declare var FileSystem: {
}
