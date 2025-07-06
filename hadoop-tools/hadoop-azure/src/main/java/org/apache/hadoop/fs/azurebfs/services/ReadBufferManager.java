package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public interface ReadBufferManager {

  void queueReadAhead(final AbfsInputStream stream, final long requestedOffset,
      final int requestedLength, TracingContext tracingContext);

  int getBlock(final AbfsInputStream stream,
      final long position,
      final int length,
      final byte[] buffer)
      throws IOException;

  ReadBuffer getNextBlockToRead() throws InterruptedException;

  void doneReading(final ReadBuffer buffer, final ReadBufferStatus result,
      final int bytesActuallyRead);

  void purgeBuffersForStream(AbfsInputStream stream);
}
