package com.codurance.ratelimiter.repository;

import java.util.concurrent.CompletableFuture;

/**
 * Data-access layer interface responsible for manage access to the distributed key-value store DB
 */
public interface DistributedKeyValueStore {

  /**
   * Increments the stored integer by delta and sets expiration in seconds only when the key is
   * created the first time. Returns the new value for that key.
   *
   * @param key Unique identifier
   * @param delta Counter
   * @param expirationSeconds Duration that the counter should persist
   * @return The accumulated count
   * @throws Exception throwing all kind of exception generically
   */
  CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expirationSeconds)
      throws Exception;
}
