/// <reference path="./throwable.d.ts" />

declare module "vertx-js/buffer" {
  export = Buffer;
}

/**
 * Most data is shuffled around inside Vert.x using buffers.
 * <p>
 * A buffer is a sequence of zero or more bytes that can read from or written to and which expands automatically as
 * necessary to accommodate any bytes written to it. You can perhaps think of a buffer as smart byte array.
 * <p>
 * Please consult the documentation for more information on buffers.
 */
interface Buffer
{

  /**
   * Returns a <code>String</code> representation of the Buffer with the <code>UTF-8</code>encoding
   */
  toString(): string;

  /**
   * Returns a <code>String</code> representation of the Buffer with the encoding specified by <code>enc</code>
   */
  toString(enc: string): string;

  /**
   * Returns a Json object representation of the Buffer
   */
  toJsonObject(): any;

  /**
   * Returns a Json array representation of the Buffer
   */
  toJsonArray(): any[];

  /**
   * Returns the <code>byte</code> at position <code>pos</code> in the Buffer.
   */
  getByte(pos: number): number;

  /**
   * Returns the unsigned <code>byte</code> at position <code>pos</code> in the Buffer, as a <code>short</code>.
   */
  getUnsignedByte(pos: number): number;

  /**
   * Returns the <code>int</code> at position <code>pos</code> in the Buffer.
   */
  getInt(pos: number): number;

  /**
   * Returns the unsigned <code>int</code> at position <code>pos</code> in the Buffer, as a <code>long</code>.
   */
  getUnsignedInt(pos: number): number;

  /**
   * Returns the <code>long</code> at position <code>pos</code> in the Buffer.
   */
  getLong(pos: number): number;

  /**
   * Returns the <code>double</code> at position <code>pos</code> in the Buffer.
   */
  getDouble(pos: number): number;

  /**
   * Returns the <code>float</code> at position <code>pos</code> in the Buffer.
   */
  getFloat(pos: number): number;

  /**
   * Returns the <code>short</code> at position <code>pos</code> in the Buffer.
   */
  getShort(pos: number): number;

  /**
   * Returns the unsigned <code>short</code> at position <code>pos</code> in the Buffer, as an <code>int</code>.
   */
  getUnsignedShort(pos: number): number;

  /**
   * Returns a copy of a sub-sequence the Buffer as a Buffer starting at position <code>start</code>
   * and ending at position <code>end - 1</code>
   */
  getBuffer(start: number, end: number): Buffer;

  /**
   * Returns a copy of a sub-sequence the Buffer as a <code>String</code> starting at position <code>start</code>
   * and ending at position <code>end - 1</code> interpreted as a String in the specified encoding
   */
  getString(start: number, end: number, enc: string): string;

  /**
   * Returns a copy of a sub-sequence the Buffer as a <code>String</code> starting at position <code>start</code>
   * and ending at position <code>end - 1</code> interpreted as a String in UTF-8 encoding
   */
  getString(start: number, end: number): string;

  /**
   * Appends the specified <code>Buffer</code> to the end of this Buffer. The buffer will expand as necessary to accommodate
   * any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendBuffer(buff: Buffer): Buffer;

  /**
   * Appends the specified <code>Buffer</code> starting at the <code>offset</code> using <code>len</code> to the end of this Buffer. The buffer will expand as necessary to accommodate
   * any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendBuffer(buff: Buffer, offset: number, len: number): Buffer;

  /**
   * Appends the specified <code>byte</code> to the end of the Buffer. The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendByte(b: number): Buffer;

  /**
   * Appends the specified unsigned <code>byte</code> to the end of the Buffer. The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendUnsignedByte(b: number): Buffer;

  /**
   * Appends the specified <code>int</code> to the end of the Buffer. The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendInt(i: number): Buffer;

  /**
   * Appends the specified unsigned <code>int</code> to the end of the Buffer. The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendUnsignedInt(i: number): Buffer;

  /**
   * Appends the specified <code>long</code> to the end of the Buffer. The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendLong(l: number): Buffer;

  /**
   * Appends the specified <code>short</code> to the end of the Buffer.The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendShort(s: number): Buffer;

  /**
   * Appends the specified unsigned <code>short</code> to the end of the Buffer.The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendUnsignedShort(s: number): Buffer;

  /**
   * Appends the specified <code>float</code> to the end of the Buffer. The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendFloat(f: number): Buffer;

  /**
   * Appends the specified <code>double</code> to the end of the Buffer. The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.
   */
  appendDouble(d: number): Buffer;

  /**
   * Appends the specified <code>String</code> to the end of the Buffer with the encoding as specified by <code>enc</code>.<p>
   * The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together.<p>
   */
  appendString(str: string, enc: string): Buffer;

  /**
   * Appends the specified <code>String str</code> to the end of the Buffer with UTF-8 encoding.<p>
   * The buffer will expand as necessary to accommodate any bytes written.<p>
   * Returns a reference to <code>this</code> so multiple operations can be appended together<p>
   */
  appendString(str: string): Buffer;

  /**
   * Sets the <code>byte</code> at position <code>pos</code> in the Buffer to the value <code>b</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setByte(pos: number, b: number): Buffer;

  /**
   * Sets the unsigned <code>byte</code> at position <code>pos</code> in the Buffer to the value <code>b</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setUnsignedByte(pos: number, b: number): Buffer;

  /**
   * Sets the <code>int</code> at position <code>pos</code> in the Buffer to the value <code>i</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setInt(pos: number, i: number): Buffer;

  /**
   * Sets the unsigned <code>int</code> at position <code>pos</code> in the Buffer to the value <code>i</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setUnsignedInt(pos: number, i: number): Buffer;

  /**
   * Sets the <code>long</code> at position <code>pos</code> in the Buffer to the value <code>l</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setLong(pos: number, l: number): Buffer;

  /**
   * Sets the <code>double</code> at position <code>pos</code> in the Buffer to the value <code>d</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setDouble(pos: number, d: number): Buffer;

  /**
   * Sets the <code>float</code> at position <code>pos</code> in the Buffer to the value <code>f</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setFloat(pos: number, f: number): Buffer;

  /**
   * Sets the <code>short</code> at position <code>pos</code> in the Buffer to the value <code>s</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setShort(pos: number, s: number): Buffer;

  /**
   * Sets the unsigned <code>short</code> at position <code>pos</code> in the Buffer to the value <code>s</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setUnsignedShort(pos: number, s: number): Buffer;

  /**
   * Sets the bytes at position <code>pos</code> in the Buffer to the bytes represented by the <code>Buffer b</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setBuffer(pos: number, b: Buffer): Buffer;

  /**
   * Sets the bytes at position <code>pos</code> in the Buffer to the bytes represented by the <code>Buffer b</code> on the given <code>offset</code> and <code>len</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setBuffer(pos: number, b: Buffer, offset: number, len: number): Buffer;

  /**
   * Sets the bytes at position <code>pos</code> in the Buffer to the value of <code>str</code> encoded in UTF-8.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setString(pos: number, str: string): Buffer;

  /**
   * Sets the bytes at position <code>pos</code> in the Buffer to the value of <code>str</code> encoded in encoding <code>enc</code>.<p>
   * The buffer will expand as necessary to accommodate any value written.
   */
  setString(pos: number, str: string, enc: string): Buffer;

  /**
   * Returns the length of the buffer, measured in bytes.
   * All positions are indexed from zero.
   */
  length(): number;

  /**
   * Returns a copy of the entire Buffer.
   */
  copy(): Buffer;

  /**
   * Returns a slice of this buffer. Modifying the content
   * of the returned buffer or this buffer affects each other's content
   * while they maintain separate indexes and marks.
   */
  slice(): Buffer;

  /**
   * Returns a slice of this buffer. Modifying the content
   * of the returned buffer or this buffer affects each other's content
   * while they maintain separate indexes and marks.
   */
  slice(start: number, end: number): Buffer;
}

declare var Buffer: {

  /**
   * Create a new, empty buffer.
   */
  buffer(): Buffer;

  /**
   * Create a new buffer given the initial size hint.
   * <p>
   * If you know the buffer will require a certain size, providing the hint can prevent unnecessary re-allocations
   * as the buffer is written to and resized.
   */
  buffer(initialSizeHint: number): Buffer;

  /**
   * Create a new buffer from a string. The string will be UTF-8 encoded into the buffer.
   */
  buffer(string: string): Buffer;

  /**
   * Create a new buffer from a string and using the specified encoding.
   * The string will be encoded into the buffer using the specified encoding.
   */
  buffer(string: string, enc: string): Buffer;
}
