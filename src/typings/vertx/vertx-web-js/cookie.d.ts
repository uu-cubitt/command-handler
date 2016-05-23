/// <reference path="../vertx-js/throwable.d.ts" />
/// <reference path="../vertx-js/vertx.d.ts" />

declare module "vertx-web-js/cookie" {
  export = Cookie;
}

/**
 * Represents an HTTP Cookie.
 * <p>
 * All cookies must have a name and a value and can optionally have other fields set such as path, domain, etc.
 * <p>
 * (Derived from io.netty.handler.codec.http.Cookie)
 */
interface Cookie
{

  /**
   * @return the name of this cookie
   */
  getName(): string;

  /**
   * @return the value of this cookie
   */
  getValue(): string;

  /**
   * Sets the value of this cookie
   */
  setValue(value: string): Cookie;

  /**
   * Sets the domain of this cookie
   */
  setDomain(domain: string): Cookie;

  /**
   * @return  the domain for the cookie
   */
  getDomain(): string;

  /**
   * Sets the path of this cookie.
   */
  setPath(path: string): Cookie;

  /**
   */
  getPath(): string;

  /**
   * Sets the maximum age of this cookie in seconds.
   * If an age of <code>0</code> is specified, this cookie will be
   * automatically removed by browser because it will expire immediately.
   * If MIN_VALUE is specified, this cookie will be removed when the
   * browser is closed.
   * If you don't set this the cookie will be a session cookie and be removed when the browser is closed.
   */
  setMaxAge(maxAge: number): Cookie;

  /**
   * Sets the security getStatus of this cookie
   */
  setSecure(secure: boolean): Cookie;

  /**
   * Determines if this cookie is HTTP only.
   * If set to true, this cookie cannot be accessed by a client
   * side script. However, this works only if the browser supports it.
   * For for information, please look
   * <a href="http://www.owasp.org/index.php/HTTPOnly">here</a>.
   */
  setHttpOnly(httpOnly: boolean): Cookie;

  /**
   * Encode the cookie to a string. This is what is used in the Set-Cookie header
   */
  encode(): string;

  /**
   * Has the cookie been changed? Changed cookies will be saved out in the response and sent to the browser.
   */
  isChanged(): boolean;

  /**
   * Set the cookie as being changed. Changed will be true for a cookie just created, false by default if just
   * read from the request
   */
  setChanged(changed: boolean): void;
}

declare var Cookie: {

  /**
   * Create a new cookie
   */
  cookie(name: string, value: string): Cookie;
}
