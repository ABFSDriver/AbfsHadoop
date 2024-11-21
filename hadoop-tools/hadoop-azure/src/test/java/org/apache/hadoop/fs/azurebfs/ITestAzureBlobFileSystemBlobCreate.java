package org.apache.hadoop.fs.azurebfs;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper.isExplicitDirectory;
import static org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper.isImplicitDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathsDoNotExist;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAzureBlobFileSystemBlobCreate extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemBlobCreate() throws Exception {
    super();
    assumeBlobServiceType();
  }

  @Test
  public void testCreateFileOnNonExistingPathWithNonExistingParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");
    int expectedConnCount = fs.getAbfsStore().getAbfsConfiguration().isBlobEndpointCreateFileOptimizationEnabled() ? 4 : 5;

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.create(filePath, false);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(expectedConnCount);

    // Verify directory state.
    assertIsFile(fs, filePath);
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs,getTestTracingContext(fs, false)))
        .describedAs("")
        .isTrue();
  }

  @Test
  public void testCreateFileOnNonExistingPathWithImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");
    int expectedConnCount = fs.getAbfsStore().getAbfsConfiguration().isBlobEndpointCreateFileOptimizationEnabled() ? 4 : 5;

    // Test Setup and file system call.
    createAzCopyFolder(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.create(filePath, false);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(expectedConnCount);

    // Verify directory state.
    assertIsFile(fs, filePath);
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs,getTestTracingContext(fs, false)))
        .describedAs("")
        .isTrue();
  }

  @Test
  public void testCreateFileOnNonExistingPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");
    int expectedConnCount = fs.getAbfsStore().getAbfsConfiguration().isBlobEndpointCreateFileOptimizationEnabled() ? 3 : 4;

    // Test Setup.
    fs.mkdirs(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.create(filePath, false);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(expectedConnCount);

    // Verify directory state.
    assertIsFile(fs, filePath);
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs,getTestTracingContext(fs, false)))
        .describedAs("")
        .isTrue();
  }

  @Test
  public void testCreateFileOnNonExistingPathWithFileParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");
    int expectedConnCount = fs.getAbfsStore().getAbfsConfiguration().isBlobEndpointCreateFileOptimizationEnabled() ? 2 : 3;

    // Test Setup.
    fs.create(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(expectedConnCount);

    // Verify directory state.
    assertIsFile(fs, filePath.getParent());
    assertPathsDoNotExist(fs, "Path Creation Should Have Failed", filePath);
  }

  @Test
  public void testCreateFileOnFilePathWithImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");

    // Test Setup.
    createAzCopyFile(filePath);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(4);

    // Verify directory state.
    assertIsFile(fs, filePath);
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs,getTestTracingContext(fs, false)))
        .describedAs("")
        .isTrue();
  }

  @Test
  public void testCreateFileOnFilePathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");

    // Test Setup.
    fs.create(filePath);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(3);

    // Verify directory state.
    assertIsFile(fs, filePath);
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs,getTestTracingContext(fs, false)))
        .describedAs("")
        .isTrue();
  }

  @Test
  public void testCreateFileOnImplicitPathWithImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");
    int expectedConnCount = fs.getAbfsStore().getAbfsConfiguration().isBlobEndpointCreateFileOptimizationEnabled() ? 1 : 2;

    // Test Setup.
    createAzCopyFolder(filePath);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(expectedConnCount);

    // Verify directory state.
    Assertions.assertThat(isImplicitDirectory(filePath, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isImplicitDirectory(filePath.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testCreateFileOnImplicitPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");
    int expectedConnCount = fs.getAbfsStore().getAbfsConfiguration().isBlobEndpointCreateFileOptimizationEnabled() ? 1 : 2;

    // Test Setup.
    createAzCopyFolder(filePath);
    fs.mkdirs(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(expectedConnCount);

    // Verify directory state.
    Assertions.assertThat(isImplicitDirectory(filePath, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testCreateFileOnEmptyExplicitPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");
    int expectedConnCount = fs.getAbfsStore().getAbfsConfiguration().isBlobEndpointCreateFileOptimizationEnabled() ? 3 : 1;

    // Test Setup.
    fs.mkdirs(filePath);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(expectedConnCount);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(filePath, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testCreateFileOnNonEmptyExplicitPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");

    // Test Setup.
    createAzCopyFolder(filePath);
    fs.mkdirs(filePath);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(1);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(filePath, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(filePath.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }
}
