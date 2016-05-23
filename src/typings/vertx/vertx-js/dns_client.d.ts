/// <reference path="./throwable.d.ts" />
/// <reference path="./mx_record.d.ts" />
/// <reference path="./srv_record.d.ts" />

declare module "vertx-js/dns_client" {
  export = DnsClient;
}

/**
 * Provides a way to asynchronously lookup information from DNS servers.
 * <p>
 * Please consult the documentation for more information on DNS clients.
 */
interface DnsClient
{

  /**
   * Try to lookup the A (ipv4) or AAAA (ipv6) record for the given name. The first found will be used.
   */
  lookup(name: string, handler: (res: string, err?: Throwable) => void): DnsClient;

  /**
   * Try to lookup the A (ipv4) record for the given name. The first found will be used.
   */
  lookup4(name: string, handler: (res: string, err?: Throwable) => void): DnsClient;

  /**
   * Try to lookup the AAAA (ipv6) record for the given name. The first found will be used.
   */
  lookup6(name: string, handler: (res: string, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve all A (ipv4) records for the given name.
   */
  resolveA(name: string, handler: (res: Array<string>, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve all AAAA (ipv6) records for the given name.
   */
  resolveAAAA(name: string, handler: (res: Array<string>, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve the CNAME record for the given name.
   */
  resolveCNAME(name: string, handler: (res: Array<string>, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve the MX records for the given name.
   */
  resolveMX(name: string, handler: (res: Array<MxRecord>, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve the TXT records for the given name.
   */
  resolveTXT(name: string, handler: (res: Array<string>, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve the PTR record for the given name.
   */
  resolvePTR(name: string, handler: (res: string, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve the NS records for the given name.
   */
  resolveNS(name: string, handler: (res: Array<string>, err?: Throwable) => void): DnsClient;

  /**
   * Try to resolve the SRV records for the given name.
   */
  resolveSRV(name: string, handler: (res: Array<SrvRecord>, err?: Throwable) => void): DnsClient;

  /**
   * Try to do a reverse lookup of an IP address. This is basically the same as doing trying to resolve a PTR record
   * but allows you to just pass in the IP address and not a valid ptr query string.
   */
  reverseLookup(ipaddress: string, handler: (res: string, err?: Throwable) => void): DnsClient;
}

declare var DnsClient: {
}
