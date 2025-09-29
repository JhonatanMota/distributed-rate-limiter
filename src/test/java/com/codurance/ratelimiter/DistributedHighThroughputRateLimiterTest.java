package com.codurance.ratelimiter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.codurance.ratelimiter.repository.DistributedKeyValueStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit tests for DistributedHighThroughputRateLimiter using a mocked DistributedKeyValueStore. */
@ExtendWith(MockitoExtension.class)
class DistributedHighThroughputRateLimiterTest {

  @Mock private DistributedKeyValueStore distributedKeyValueStore;
  private DistributedHighThroughputRateLimiter limiter;

  @BeforeEach
  void setUp() {
    ConcurrentHashMap<String, AtomicInteger> remote = new ConcurrentHashMap<>();
    try {
      when(distributedKeyValueStore.incrementByAndExpire(anyString(), anyInt(), anyInt()))
          .thenAnswer(
              invocation -> {
                String key = invocation.getArgument(0); // Extract the 1st argument (key)
                int delta = invocation.getArgument(1); // Extract the 2nd argument (delta)

                // Update the "remote storage" simulation
                remote.computeIfAbsent(key, k -> new AtomicInteger(0));
                int newValue = remote.get(key).addAndGet(delta);

                // Return the updated value wrapped in a CompletableFuture
                return CompletableFuture.completedFuture(newValue);
              });
    } catch (Exception e) {
      fail("Mock setup should not throw: " + e.getMessage());
    }
    limiter = new DistributedHighThroughputRateLimiter(distributedKeyValueStore);
  }

  @Test
  void allowUnderLimit() {
    String client = "xyz";
    int limit = 50;
    for (int i = 0; i < limit; i++) {
      assertTrue(limiter.isAllowed(client, limit).join(), "should be allowed while under limit");
    }
  }

  @Test
  void allowOverLimit() throws Exception {
    String client = "abc";
    int limit = 0;

    when(distributedKeyValueStore.incrementByAndExpire(client, 0, 60))
        .thenReturn(CompletableFuture.completedFuture(1));

    assertFalse(limiter.isAllowed(client, limit).join());
  }
}
