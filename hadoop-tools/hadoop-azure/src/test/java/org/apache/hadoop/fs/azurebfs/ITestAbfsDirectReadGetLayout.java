package org.apache.hadoop.fs.azurebfs;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.assertj.core.api.Assertions.assertThat;

public class ITestAbfsDirectReadGetLayout
    extends AbstractAbfsIntegrationTest {

  public ITestAbfsDirectReadGetLayout() throws Exception {
    super();
  }

  @Test
  public void testDfsGetLayoutWithDataHandle() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/direct-read-get-layout.bin");

    final byte[] data = new byte[1024];

    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }

    try {
      // Create a 1024-byte test file.
      try (FSDataOutputStream out = fs.create(path, true)) {
        out.write(data);
      }

      final AzureBlobFileSystemStore store = fs.getAbfsStore();

      // Use the DFS client.
      final AbfsClient client = store.getClient();

      // Request layout for bytes 0-511.
      final AbfsRestOperation operation = client.getBlobLayout(
          path.toString(),
          0,
          511,
          null,
          null,
          getTestTracingContext(fs, false));

      assertThat(operation).isNotNull();
      assertThat(operation.getResult()).isNotNull();

    } finally {
      fs.delete(path, false);
    }
  }
}