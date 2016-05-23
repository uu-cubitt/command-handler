/// <reference path="./throwable.d.ts" />
/// <reference path="./buffer.d.ts" />

declare module "vertx-js/record_parser" {
  export = RecordParser;
}

/**
 * A helper class which allows you to easily parse protocols which are delimited by a sequence of bytes, or fixed
 * size records.
 * <p>
 * Instances of this class take as input Buffer instances containing raw bytes,
 * and output records.
 * <p>
 * For example, if I had a simple ASCII text protocol delimited by '\n' and the input was the following:
 * <p>
 * <pre>
 * buffer1:HELLO\nHOW ARE Y
 * buffer2:OU?\nI AM
 * buffer3: DOING OK
 * buffer4:\n
 * </pre>
 * Then the output would be:<p>
 * <pre>
 * buffer1:HELLO
 * buffer2:HOW ARE YOU?
 * buffer3:I AM DOING OK
 * </pre>
 * Instances of this class can be changed between delimited mode and fixed size record mode on the fly as
 * individual records are read, this allows you to parse protocols where, for example, the first 5 records might
 * all be fixed size (of potentially different sizes), followed by some delimited records, followed by more fixed
 * size records.
 * <p>
 * Instances of this class can't currently be used for protocols where the text is encoded with something other than
 * a 1-1 byte-char mapping.
 * <p>
 * Please see the documentation for more information.
 */
interface RecordParser
{
  setOutput(output: (e: Buffer) => void): void;

  /**
   * Flip the parser into delimited mode, and where the delimiter can be represented
   * by the String <code>delim</code> encoded in latin-1 . Don't use this if your String contains other than latin-1 characters.
   * <p>
   * This method can be called multiple times with different values of delim while data is being parsed.
   */
  delimitedMode(delim: string): void;

  /**
   * Flip the parser into delimited mode, and where the delimiter can be represented
   * by the delimiter <code>delim</code>.
   * <p>
   * This method can be called multiple times with different values of delim while data is being parsed.
   */
  delimitedMode(delim: Buffer): void;

  /**
   * Flip the parser into fixed size mode, where the record size is specified by <code>size</code> in bytes.
   * <p>
   * This method can be called multiple times with different values of size while data is being parsed.
   */
  fixedSizeMode(size: number): void;

  /**
   * This method is called to provide the parser with data.
   */
  handle(buffer: Buffer): void;
}

declare var RecordParser: {

  /**
   * Create a new <code>RecordParser</code> instance, initially in delimited mode, and where the delimiter can be represented
   * by the String <code></code> delim endcoded in latin-1 . Don't use this if your String contains other than latin-1 characters.
   * <p>
   * <code>output</code> Will receive whole records which have been parsed.
   */
  newDelimited(delim: string, output: (e: Buffer) => void): RecordParser;

  /**
   * Create a new <code>RecordParser</code> instance, initially in delimited mode, and where the delimiter can be represented
   * by the <code>Buffer</code> delim.
   * <p>
   * <code>output</code> Will receive whole records which have been parsed.
   */
  newDelimited(delim: Buffer, output: (e: Buffer) => void): RecordParser;

  /**
   * Create a new <code>RecordParser</code> instance, initially in fixed size mode, and where the record size is specified
   * by the <code>size</code> parameter.
   * <p>
   * <code>output</code> Will receive whole records which have been parsed.
   */
  newFixed(size: number, output: (e: Buffer) => void): RecordParser;
}
