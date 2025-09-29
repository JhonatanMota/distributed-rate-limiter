package com.codurance.ratelimiter;

import com.codurance.ratelimiter.repository.DistributedKeyValueStore;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistributedHighThroughputRateLimiter {

  // The rate limit period can be assumed to be fixed at 60 seconds and will never change.
  private static final int RATE_LIMIT_IN_SECONDS = 60;

  private final DistributedKeyValueStore distributedKeyValueStore;
  private final ConcurrentHashMap<String, AtomicInteger> concurrentHashMap =
      new ConcurrentHashMap<>();

  public DistributedHighThroughputRateLimiter(DistributedKeyValueStore distributedKeyValueStore) {
    this.distributedKeyValueStore = Objects.requireNonNull(distributedKeyValueStore);
  }

  /**
   * Returns a CompletableFuture<Boolean> that completes immediately with our local decision. The
   * decision is "approximate" as required; exact global accuracy is not guaranteed.
   */
  public CompletableFuture<Boolean> isAllowed(@NotNull String key, @Positive int limit) {

    boolean allowed = false;
    try {
      concurrentHashMap.computeIfAbsent(key, k -> new AtomicInteger(0));

      CompletableFuture<Integer> counter =
          distributedKeyValueStore.incrementByAndExpire(
              key, concurrentHashMap.get(key).get(), RATE_LIMIT_IN_SECONDS);

      int counterValue = counter.get();
      concurrentHashMap.compute(key, (k, v) -> new AtomicInteger(counterValue));
      allowed = (counterValue <= limit);

    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }

    return CompletableFuture.completedFuture(allowed);
  }
}
