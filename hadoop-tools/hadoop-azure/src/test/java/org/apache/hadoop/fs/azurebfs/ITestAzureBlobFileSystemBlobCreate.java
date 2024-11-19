package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper.isExplicitDirectory;
import static org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper.isImplicitDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathsDoNotExist;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

public class ITestAzureBlobFileSystemBlobCreate extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemBlobCreate() throws Exception {
    super();
  }

  @Test
  public void testCreateFileOnNonExistingPathWithNonExistingParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = new Path("/dir/file.txt");

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.create(filePath, false);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(4);

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

    // Test Setup and file system call.
    createAzCopyFolder(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.create(filePath, false);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(4);

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

    // Test Setup.
    fs.mkdirs(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.create(filePath, false);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(3);

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

    // Test Setup.
    fs.create(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(2);

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

    // Test Setup.
    createAzCopyFolder(filePath);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(1);

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

    // Test Setup.
    createAzCopyFolder(filePath);
    fs.mkdirs(filePath.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(1);

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

    // Test Setup.
    fs.mkdirs(filePath);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.create(filePath, false));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(3);

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

  @Test
  public void testMkdirOnNonExistingPathWithNonExistingParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.mkdirs(path);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(4);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(path, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnNonExistingPathWithImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    createAzCopyFolder(path);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.mkdirs(path);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(4);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(path, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnNonExistingPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    fs.mkdirs(path.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.mkdirs(path);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(3);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(path, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnNonExistingPathWithFileParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    fs.create(path.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(path));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(2);

    // Verify directory state.
    assertIsFile(fs, path.getParent());
    assertPathsDoNotExist(fs, "Path Creation Should Have Failed", path);
  }

  @Test
  public void testMkdirOnFilePathWithImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    createAzCopyFile(path);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(path));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(1);

    // Verify directory state.
    assertIsFile(fs, path);
    Assertions.assertThat(isImplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnFilePathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    fs.create(path);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(path));
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(1);

    // Verify directory state.
    assertIsFile(fs, path);
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnImplicitPathWithImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    createAzCopyFolder(path);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.mkdirs(path);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(4);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(path, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnImplicitPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    createAzCopyFolder(path);
    fs.mkdirs(path.getParent());

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.mkdirs(path);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(3);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(path, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnEmptyExplicitPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir");

    // Test Setup.
    fs.mkdirs(path);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.mkdirs(path);
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(3);

    // Verify directory state.
    Assertions.assertThat(isExplicitDirectory(path, fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }

  @Test
  public void testMkdirOnNonEmptyExplicitPathWithExplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/parent/dir/file.txt");

    // Test Setup.
    fs.create(path);

    // Verify Total Number of network calls.
    long oldConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    fs.mkdirs(path.getParent());
    long newConnCount = fs.getInstrumentationMap().get(CONNECTIONS_MADE.getStatName());
    Assertions.assertThat(newConnCount - oldConnCount).describedAs("").isEqualTo(3);

    // Verify directory state.
    assertIsFile(fs, path);
    Assertions.assertThat(isExplicitDirectory(path.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
    Assertions.assertThat(isExplicitDirectory(path.getParent().getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("").isTrue();
  }
}
