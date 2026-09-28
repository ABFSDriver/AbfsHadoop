package org.apache.hadoop.fs.azurebfs.services;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT;

public class BlobLayoutCacheTest {

  private BlobLayoutCache cache;

  private final long contentLength = 100;

  private BlobLayoutResponse layoutResponse;

  @BeforeEach
  public void setUp() {
    cache = BlobLayoutCache.getInstance(1L,
        DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);

    this.layoutResponse = new BlobLayoutResponse();
    layoutResponse.setRanges(List.of(new BlobLayoutResponse.Range(0, 9, 0, null,  0L),
        new BlobLayoutResponse.Range(10, 19, 1, null, 0L),
        new BlobLayoutResponse.Range(20, 29, 2, null, 0L),
        new BlobLayoutResponse.Range(30, 39, 3, null, 0L)));
    layoutResponse.setEndpoints(
        Set.of(new BlobLayoutResponse.Endpoint(0, "host-a"),
            new BlobLayoutResponse.Endpoint(1, "host-b"),
            new BlobLayoutResponse.Endpoint(2, "host-c"),
            new BlobLayoutResponse.Endpoint(3, "host-d")));
  }

  /**
   * Tests registering and deregistering a stream in the BlobLayoutCache.
   * <p>
   * Verifies that after registering a stream, the blob layout is empty if no elements are added.
   * After deregistering, the blob layout remains empty, and no exceptions are thrown.
   */
  @Test
  public void testRegisterAndDeregisterStream() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    List<BlobLayout.BlobRange> blobRangeList = cache.getBlobLayout(eTag, 0,
        4);
    Assertions.assertThat(blobRangeList)
        .describedAs("List should be empty in case no element is added.")
        .hasSize(0);
    cache.deregisterStream(eTag);
    blobRangeList = cache.getBlobLayout(eTag, 0, 4);
    Assertions.assertThat(blobRangeList)
        .describedAs(
            "In case stream is deregistered, doesn't mean data is removed immediately from the cache.")
        .hasSize(0);
    // No direct assertion, but should not throw
  }

  /**
   * Tests putting and retrieving a blob layout in the BlobLayoutCache.
   * <p>
   * Verifies that after putting a blob layout, the correct ranges and hosts are returned
   * for a specified range.
   */
  @Test
  public void testPutBlobLayoutAndGetBlobLayout() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);

    // If start = 0, and end = 10 (both inclusive), get blob layout will return two entries
    // 0-9 -> host-a
    // 10-10 -> host-b
    List<BlobLayout.BlobRange> blobRangeList = cache.getBlobLayout(eTag, 0, 10);
    Assertions.assertThat(blobRangeList)
        .describedAs("List should contains 2 elements.")
        .hasSize(2);
    Assertions.assertThat(blobRangeList.get(0).host())
        .describedAs("First range should be host-a.")
        .isEqualTo("host-a");
    Assertions.assertThat(blobRangeList.get(1).host())
        .describedAs("Second range should be host-b.")
        .isEqualTo("host-b");
    cache.deregisterStream(eTag);
  }

  /**
   * Tests gap detection when content length is greater than the last byte present in the layout.
   * <p>
   * Verifies that gaps are correctly identified and returned for the specified range.
   */
  @Test
  public void testGapWithContentLengthGreaterThanLastBytePresent() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);

    // If start = 35, and end = 45 (both inclusive), get Gaps will return one entry
    // 40-45 -> null
    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 35, 45);
    Assertions.assertThat(gaps)
        .describedAs("List should contains 1 elements.")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start())
        .describedAs("Gap should start from 40.")
        .isEqualTo(40);
    Assertions.assertThat(gaps.get(0).end())
        .describedAs("Gap should end at 45.")
        .isEqualTo(45);
    cache.deregisterStream(eTag);
  }

  /**
   * Tests gap detection when content length is equal to the last byte present in the layout.
   * <p>
   * Verifies that no gaps are returned, and the correct blob range and host are returned for the specified range.
   */
  @Test
  public void testGapWithContentLengthEqualToLastBytePresent() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, 40);
    cache.putBlobLayout(eTag, layoutResponse, 40);

    // If start = 35, and end = 45 (both inclusive), get Gaps will return zero entry
    // as file size is not more than 39, so there is no gap.
    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 35, 45);
    Assertions.assertThat(gaps)
        .describedAs("List should contains 0 elements.")
        .hasSize(0);
    // This will return 1 entry for 35-39 -> host-d
    List<BlobLayout.BlobRange> blobRangeList = cache.getBlobLayout(eTag, 35,
        45);
    Assertions.assertThat(blobRangeList)
        .describedAs("List should contains 1 elements.")
        .hasSize(1);
    Assertions.assertThat(blobRangeList.get(0).host())
        .describedAs("Host should be host-d.")
        .isEqualTo("host-d");
    cache.deregisterStream(eTag);
  }

  /**
   * Tests putting blob layouts with gaps and filling those gaps.
   * <p>
   * Verifies that gaps are correctly identified, filled, and the correct blob ranges and hosts are returned.
   */
  @Test
  public void testPutBlobLayoutWithGap() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);
    BlobLayoutResponse blobLayoutResponse = new BlobLayoutResponse();
    blobLayoutResponse.setRanges(
        List.of(new BlobLayoutResponse.Range(45, 54, 0, null, 0L)));
    blobLayoutResponse.setEndpoints(
        Set.of(new BlobLayoutResponse.Endpoint(0, "host-a")));
    cache.putBlobLayout(eTag, blobLayoutResponse, contentLength);

    // Layout present for 35-39 & 45-54.
    // Gap: 40-44 & 55-65
    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 35, 65);
    Assertions.assertThat(gaps)
        .describedAs("List should contains 2 elements.")
        .hasSize(2);
    Assertions.assertThat(gaps.get(0).start())
        .describedAs("First gap should start from 40.")
        .isEqualTo(40);
    Assertions.assertThat(gaps.get(0).end())
        .describedAs("First gap should end to 44.")
        .isEqualTo(44);
    Assertions.assertThat(gaps.get(1).start())
        .describedAs("Second gap should start from 55.")
        .isEqualTo(55);
    Assertions.assertThat(gaps.get(1).end())
        .describedAs("Second gap should end to 65.")
        .isEqualTo(65);

    // Gap fill
    blobLayoutResponse = new BlobLayoutResponse();
    blobLayoutResponse.setRanges(
        List.of(new BlobLayoutResponse.Range(35, 44, 0, null, 0L)));
    blobLayoutResponse.setEndpoints(
        Set.of(new BlobLayoutResponse.Endpoint(0, "host-d")));
    cache.putBlobLayout(eTag, blobLayoutResponse, contentLength);
    gaps = cache.getGaps(eTag, 35, 65);
    // Only one gap remaining - 55-65
    Assertions.assertThat(gaps)
        .describedAs("List should contains 0 elements.")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start())
        .describedAs("Gap should start from 55.")
        .isEqualTo(55);
    Assertions.assertThat(gaps.get(0).end())
        .describedAs("Gap should end to 65.")
        .isEqualTo(65);

    List<BlobLayout.BlobRange> blobRanges = cache.getBlobLayout(eTag, 35, 65);
    // 1st - 35-44 -> host-d
    // 2nd - 45-54 -> host-a
    Assertions.assertThat(blobRanges)
        .describedAs("List should contains 2 elements.")
        .hasSize(2);
    Assertions.assertThat(blobRanges.get(0).host())
        .describedAs("First range host should be host-d.")
        .isEqualTo("host-d");
    Assertions.assertThat(blobRanges.get(1).host())
        .describedAs("Second range host should be host-a.")
        .isEqualTo("host-a");
    cache.deregisterStream(eTag);
  }

  /**
   * Tests cache eviction behavior in BlobLayoutCache.
   * <p>
   * Verifies that after cache eviction, blob layout retrieval returns null.
   */
  @Test
  public void testCacheEviction() throws InterruptedException {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);
    List<BlobLayout.BlobRange> blobRanges = cache.getBlobLayout(eTag, 0, 10);
    Assertions.assertThat(blobRanges)
        .describedAs("List should contains 2 elements.")
        .isNotNull();
    cache.deregisterStream(eTag);
    // wait for some time such that cache will be evicted.
    Thread.sleep(TimeUnit.MINUTES.toMillis(1) + 1000);
    blobRanges = cache.getBlobLayout(eTag, 0, 10);
    Assertions.assertThat(blobRanges)
        .describedAs("List should be null.")
        .isNull();
  }

  /**
   * Tests bridge gap detection with back fill in BlobLayoutCache.
   * <p>
   * Verifies that the correct bridge gap is identified and returned when back filling.
   */
  @Test
  public void testBridgeGapWithBackFill() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    // Since no data is present, contentLength = 100, pos = 95, maxFetch = 66
    // pos + maxFetch > contentLength, so the gap will be from 36-99.
    BlobLayout.BlobRange blobRange = cache.getBridgeGap(eTag, 95, 64);
    Assertions.assertThat(blobRange)
        .describedAs("Bridge gap should be present.")
        .isNotNull();
    Assertions.assertThat(blobRange.start())
        .describedAs("Bridge gap should be start.")
        .isEqualTo(36);
    Assertions.assertThat(blobRange.end())
        .describedAs("Bridge gap should be end.")
        .isEqualTo(99);
  }

  /**
   * Tests bridge gap detection with forward fill in BlobLayoutCache.
   * <p>
   * Verifies that the correct bridge gap is identified and returned when forward filling.
   */
  @Test
  public void testBridgeGapWithForwardFill() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    // Since no data is present, contentLength = 100, pos = 30, maxFetch = 66
    // pos + maxFetch < contentLength, so the gap will be from 30-93.
    BlobLayout.BlobRange blobRange = cache.getBridgeGap(eTag, 30, 64);
    Assertions.assertThat(blobRange)
        .describedAs("Bridge gap should be present.")
        .isNotNull();
    Assertions.assertThat(blobRange.start())
        .describedAs("Bridge gap should be start.")
        .isEqualTo(30);
    Assertions.assertThat(blobRange.end())
        .describedAs("Bridge gap should be end.")
        .isEqualTo(93);
  }

  /**
   * Tests that processInFlightPromises correctly initializes a new list for a new eTag.
   */
  @Test
  public void testProcessInFlightPromisesInitialization() {
    String eTag = "test-new-etag";
    CompletableFuture<Void> result = cache.processInFlightPromises(eTag, (promiseList) -> {
      Assertions.assertThat(promiseList)
          .describedAs("Promise list should be initialized if null.")
          .isNotNull();
      Assertions.assertThat(promiseList).isEmpty();
      return CompletableFuture.completedFuture(null);
    });
    Assertions.assertThat(result).isCompleted();
  }

  /**
   * Tests that updates to the promiseList persist across multiple calls for the same eTag.
   */
  @Test
  public void testProcessInFlightPromisesPersistence() {
    String eTag = "test-persistence-etag";
    CompletableFuture<Void> firstFuture = new CompletableFuture<>();

    // First call: Add a promise
    cache.processInFlightPromises(eTag, (list) -> {
      list.add(new BlobLayoutCache.InFlightPromise(0, 10, firstFuture));
      return CompletableFuture.completedFuture(null);
    });

    // Second call: Verify promise exists
    cache.processInFlightPromises(eTag, (list) -> {
      Assertions.assertThat(list).hasSize(1);
      Assertions.assertThat(list.get(0).start()).isEqualTo(0);
      return CompletableFuture.completedFuture(null);
    });
  }

  /**
   * Tests the error propagation from the provided action lambda to the returned future.
   */
  @Test
  public void testProcessInFlightPromisesErrorPropagation() {
    String eTag = "test-error-etag";
    RuntimeException expectedEx = new RuntimeException("Action failed");

    CompletableFuture<Void> result = cache.processInFlightPromises(eTag, (list) -> {
      CompletableFuture<Void> failed = new CompletableFuture<>();
      failed.completeExceptionally(expectedEx);
      return failed;
    });

    Assertions.assertThat(result).isCompletedExceptionally();
    Assertions.assertThatThrownBy(result::get)
        .hasCause(expectedEx);
  }

  /**
   * Tests thread safety by simulating multiple threads accessing the same eTag.
   * ConcurrentHashMap.compute should serialize these operations.
   */
  @Test
  public void testProcessInFlightPromisesConcurrency() throws Exception {
    String eTag = "concurrent-etag";
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          cache.processInFlightPromises(eTag, (list) -> {
            // Simulate some work inside the compute block
            list.add(new BlobLayoutCache.InFlightPromise(0, 1, new CompletableFuture<>()));
            return CompletableFuture.completedFuture(null);
          });
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(5, TimeUnit.SECONDS);

    // Verify all additions were successful
    cache.processInFlightPromises(eTag, (list) -> {
      Assertions.assertThat(list).hasSize(threadCount);
      return CompletableFuture.completedFuture(null);
    });

    executor.shutdown();
  }

  /**
   * Tests that the action can return a future that completes later (Async).
   */
  @Test
  public void testProcessInFlightPromisesAsyncAction() {
    String eTag = "async-etag";
    CompletableFuture<Void> actionResult = new CompletableFuture<>();

    CompletableFuture<Void> returnedFuture = cache.processInFlightPromises(eTag, (list) -> actionResult);

    Assertions.assertThat(returnedFuture).isNotCompleted();

    actionResult.complete(null);

    Assertions.assertThat(returnedFuture).isCompleted();
  }

  /**
   * Verifies that invalidating a cached range turns it into a gap.
   */
  @Test
  public void testInvalidateRangesMakesRangeAGap() {
    String key = "invalidate-gap-" + System.nanoTime();
    cache.registerStream(key, 100);
    cache.putBlobLayout(key, handleLayout(0, 99, "handle-1"), 100);

    Assertions.assertThat(cache.getGaps(key, 0, 99))
        .describedAs("A fully cached range should have no gaps")
        .isEmpty();

    cache.invalidateRanges(key, 0, 99);

    Assertions.assertThat(cache.getGaps(key, 0, 99))
        .describedAs("The invalidated range should be a gap")
        .containsExactly(new BlobLayout.BlobRange(0, 99, null));
    cache.deregisterStream(key);
  }

  /**
   * Verifies that invalidation removes only the range coverage and keeps the
   * cache entry. The entry holds the stream registration, so dropping it
   * would allow eviction while streams are still open.
   */
  @Test
  public void testInvalidateRangesKeepsCacheEntry() {
    String key = "invalidate-keeps-entry-" + System.nanoTime();
    cache.registerStream(key, 100);
    cache.putBlobLayout(key, handleLayout(0, 99, "handle-1"), 100);

    cache.invalidateRanges(key, 0, 99);

    Assertions.assertThat(cache.getBlobLayout(key, 0, 99))
        .describedAs("The entry should still exist, with no ranges")
        .isNotNull()
        .isEmpty();
    cache.deregisterStream(key);
  }

  /**
   * Verifies that a layout fetched after invalidation replaces the old range
   * and carries the new handle.
   */
  @Test
  public void testRefreshedLayoutReplacesInvalidatedRange() {
    String key = "invalidate-refresh-" + System.nanoTime();
    cache.registerStream(key, 100);
    cache.putBlobLayout(key, handleLayout(0, 99, "old-handle"), 100);

    cache.invalidateRanges(key, 0, 99);
    cache.putBlobLayout(key, handleLayout(0, 99, "new-handle"), 100);

    List<BlobLayout.BlobRange> ranges = cache.getBlobLayout(key, 0, 99);
    Assertions.assertThat(ranges)
        .describedAs("The refreshed layout should cover the range")
        .hasSize(1);
    Assertions.assertThat(ranges.get(0).handle())
        .describedAs("The refreshed range should carry the new handle")
        .isEqualTo("new-handle");
    Assertions.assertThat(cache.getGaps(key, 0, 99))
        .describedAs("There should be no gaps after the refresh")
        .isEmpty();
    cache.deregisterStream(key);
  }

  /**
   * Verifies that invalidating one key does not affect another. Streams with
   * and without Direct Read use different keys for the same eTag.
   */
  @Test
  public void testInvalidateRangesDoesNotAffectOtherKeys() {
    String eTag = "invalidate-isolation-" + System.nanoTime();
    String handleKey = eTag + "#datahandle";
    cache.registerStream(eTag, 100);
    cache.registerStream(handleKey, 100);
    cache.putBlobLayout(eTag, handleLayout(0, 99, null), 100);
    cache.putBlobLayout(handleKey, handleLayout(0, 99, "handle-1"), 100);

    cache.invalidateRanges(handleKey, 0, 99);

    Assertions.assertThat(cache.getGaps(eTag, 0, 99))
        .describedAs("The entry without handles should be unaffected")
        .isEmpty();
    Assertions.assertThat(cache.getGaps(handleKey, 0, 99))
        .describedAs("The entry with handles should now have a gap")
        .hasSize(1);
    cache.deregisterStream(eTag);
    cache.deregisterStream(handleKey);
  }

  /**
   * Verifies that invalidating a null or unknown key is a no-op.
   */
  @Test
  public void testInvalidateRangesWithUnknownOrNullKeyIsNoOp() {
    cache.invalidateRanges(null, 0, 99);
    cache.invalidateRanges("unknown-key-" + System.nanoTime(), 0, 99);
    // Reaching this line without an exception is the pass condition.
  }

  /**
   * Builds a single-range layout on host-a with the given handle, or no
   * handle when {@code handle} is null.
   */
  private static BlobLayoutResponse handleLayout(long start, long end,
      String handle) {
    BlobLayoutResponse response = new BlobLayoutResponse();
    response.setRanges(List.of(new BlobLayoutResponse.Range(
        start, end, 0, handle,
        handle == null
            ? 0L
            : System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5))));
    response.setEndpoints(Set.of(new BlobLayoutResponse.Endpoint(0, "host-a")));
    return response;
  }
}
