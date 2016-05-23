/// <reference path="./throwable.d.ts" />
/// <reference path="./async_map.d.ts" />
/// <reference path="./lock.d.ts" />
/// <reference path="./local_map.d.ts" />
/// <reference path="./counter.d.ts" />

declare module "vertx-js/shared_data" {
  export = SharedData;
}

/**
 * Shared data allows you to share data safely between different parts of your application in a safe way.
 * <p>
 * Shared data provides:
 * <ul>
 *   <li>Cluster wide maps which can be accessed from any node of the cluster</li>
 *   <li>Cluster wide locks which can be used to give exclusive access to resources across the cluster</li>
 *   <li>Cluster wide counters used to maintain counts consistently across the cluster</li>
 *   <li>Local maps for sharing data safely in the same Vert.x instance</li>
 * </ul>
 * <p>
 * Please see the documentation for more information.
 */
interface SharedData
{

  /**
   * Get the cluster wide map with the specified name. The map is accessible to all nodes in the cluster and data
   * put into the map from any node is visible to to any other node.
   */
  getClusterWideMap(name: string, resultHandler: (res: AsyncMap, err?: Throwable) => void): void;

  /**
   * Get a cluster wide lock with the specified name. The lock will be passed to the handler when it is available.
   */
  getLock(name: string, resultHandler: (res: Lock, err?: Throwable) => void): void;

  /**
   * Like getLock but specifying a timeout. If the lock is not obtained within the timeout
   * a failure will be sent to the handler
   */
  getLockWithTimeout(name: string, timeout: number, resultHandler: (res: Lock, err?: Throwable) => void): void;

  /**
   * Get a cluster wide counter. The counter will be passed to the handler.
   */
  getCounter(name: string, resultHandler: (res: Counter, err?: Throwable) => void): void;

  /**
   * Return a <code>LocalMap</code> with the specific <code>name</code>.
   */
  getLocalMap(name: string): LocalMap;
}

declare var SharedData: {
}
