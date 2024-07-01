/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;

/**
 * Helper class to create a file or folder in Azure Blob Storage using Azcopy tool.
 * <a href="https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10">
 * Azcopy</a> is a command-line utility tool to copy blobs or files to or from a storage account.
 * It uses Blob Endpoint and ends up creating implicit paths in the storage account.
 * We will leverage this tool to create implicit paths in storage account for testing purposes.
 */

public class AzcopyToolHelper {

  private File hadoopAzureDir;
  private String azcopyDirPath;
  private String azcopyExecutablePath;
  private String fileCreationScriptPath;
  private String folderCreationScriptPath;
  private String sasToken;
  private boolean initialized = false;

  private final String USER_DIR_SYSTEM_PROPERTY = "user.dir";
  private final String HADOOP_AZURE_DIR = "hadoop-azure";
  private final String AZCOPY_DIR_NAME = "azcopy";
  private final String AZCOPY_EXECUTABLE_NAME = "azcopy";
  private final String FILE_CREATION_SCRIPT_NAME = "createAzcopyFile.sh";
  private final String FOLDER_CREATION_SCRIPT_NAME = "createAzcopyFolder.sh";
  private final String DIR_NOT_FOUND_ERROR = " directory not found";
  private final String AZCOPY_DOWNLOADED_DIR_NAME = "/azcopy_linux_amd64_*/* ";
  private final String AZCOPY_DOWNLOADED_TAR_NAME = "/azcopy_linux_amd64_* azcopy.tar.gz";

  private final String AZCOPY_CMD_SHELL = "bash";
  private final String AZCOPY_CMD_OPTION = "-c";
  private final String AZCOPY_DOWNLOAD_URL = "https://aka.ms/downloadazcopy-v10-linux";
  private final String AZCOPY_DOWNLOAD_CMD = "wget " + AZCOPY_DOWNLOAD_URL + " -O azcopy.tar.gz" + " --no-check-certificate";
  private final String EXTRACT_CMD = "tar -xf azcopy.tar.gz -C ";
  private final String MOVE_CMD = "mv ";
  private final String REMOVE_CMD = "rm -rf ";
  private final String CHMOD_CMD = "chmod +x ";
  private final char QUESTION_MARK = '?';

  public AzcopyToolHelper(String sasToken) {
    this.sasToken = sasToken.charAt(0) == QUESTION_MARK ? sasToken : QUESTION_MARK + sasToken;
  }

  public synchronized void initialize() throws IOException, InterruptedException {
    if (initialized) {
      return;
    }
    hadoopAzureDir = findHadoopAzureDir();
    azcopyDirPath = hadoopAzureDir.getAbsolutePath() + FORWARD_SLASH + AZCOPY_DIR_NAME;
    azcopyExecutablePath = azcopyDirPath + FORWARD_SLASH + AZCOPY_EXECUTABLE_NAME;

    // Synchronized on directory creation.
    // If multiple process try to create directory, only one will succeed and others will return.
    downloadAzcopyExecutableIfNotPresent();

    // Change working directory to the hadoop-azure directory.
    System.setProperty(USER_DIR_SYSTEM_PROPERTY, hadoopAzureDir.getAbsolutePath());

    // Create shell scripts for file creation with exclusive file lock.
    fileCreationScriptPath = azcopyDirPath + FORWARD_SLASH + FILE_CREATION_SCRIPT_NAME;
    if(!fileExists(fileCreationScriptPath)) {
      String fileCreationScriptContent = "blobPath=$1\n"
          + "echo $blobPath\n"
          + azcopyExecutablePath + " copy \"" + azcopyDirPath
          + "/NOTICE.txt\" $blobPath\n";
      createShellScript(fileCreationScriptPath, fileCreationScriptContent);
      setExecutablePermission(fileCreationScriptPath);
    }

    // Create shell scripts for folder creation with exclusive file lock.
    folderCreationScriptPath = azcopyDirPath + FORWARD_SLASH + FOLDER_CREATION_SCRIPT_NAME;
    if(!fileExists(fileCreationScriptPath)) {
      String folderCreationScriptContent = "blobPath=$1\n"
          + azcopyExecutablePath + " copy \"" + azcopyDirPath
          + "\" $blobPath --recursive\n";
      createShellScript(folderCreationScriptPath, folderCreationScriptContent);
      setExecutablePermission(folderCreationScriptPath);
    }

    initialized = true;
  }

  /**
   * Create a file with implicit parent in the container using Azcopy tool.
   * @param absolutePathToBeCreated absolute path to be created.
   * @throws Exception if file creation fails.
   */
  public void createFileUsingAzcopy(String absolutePathToBeCreated) throws Exception {
    absolutePathToBeCreated = absolutePathToBeCreated.replace(
        ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME) + sasToken;
    if (absolutePathToBeCreated != null) {
      runShellScript(fileCreationScriptPath, absolutePathToBeCreated);
    }
  }

  /**
   * Create a folder in the container using Azcopy tool.
   * @param absolutePathToBeCreated absolute path to be created.
   * @throws Exception
   */
  public void createFolderUsingAzcopy(String absolutePathToBeCreated) throws Exception {
    absolutePathToBeCreated = absolutePathToBeCreated.replace(
        ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME) + sasToken;
    if (absolutePathToBeCreated != null) {
      runShellScript(folderCreationScriptPath, absolutePathToBeCreated);
    }
  }

  private File findHadoopAzureDir() throws FileNotFoundException {
    // Find the hadoop-azure directory from the current working directory.
    File hadoopAzureDir;
    File currentDir = new File(System.getProperty(USER_DIR_SYSTEM_PROPERTY));
    if (!currentDir.isDirectory() && !currentDir.getName().equals(HADOOP_AZURE_DIR)) {
      hadoopAzureDir = findHadoopAzureDir(currentDir);
      if (hadoopAzureDir == null) {
        throw new FileNotFoundException(HADOOP_AZURE_DIR + DIR_NOT_FOUND_ERROR);
      }
    } else {
      hadoopAzureDir = currentDir;
    }
    return hadoopAzureDir;
  }

  private File findHadoopAzureDir(File dir) {
    if (dir == null) {
      return null;
    }
    File[] files = dir.listFiles();
    if (files == null) {
      return null;
    }
    for (File file : files) {
      if (file.isDirectory() && file.getName().equals(HADOOP_AZURE_DIR)) {
        return file;
      } else {
        File hadoopAzureDir = findHadoopAzureDir(file);
        if (hadoopAzureDir != null) {
          return hadoopAzureDir;
        }
      }
    }
    return null;
  }

  private void downloadAzcopyExecutableIfNotPresent()
      throws IOException, InterruptedException {
    // Check if azcopy directory is present in the hadoop-azure directory.
    // If not present, create a directory and stop other process here only.
    File azcopyDir = new File(azcopyDirPath);
    if (!azcopyDir.exists()) {
      azcopyDir.mkdir();
    } else {
      return;
    }

    // Check if azcopy tool is present in the azcopy directory.
    File azcopyFile = new File(azcopyExecutablePath);
    if (!azcopyFile.exists()) {
      // If azcopy tool is not present, download and extract it
      String[] downloadCmdArr = {AZCOPY_CMD_SHELL,
          AZCOPY_CMD_OPTION, AZCOPY_DOWNLOAD_CMD};
      Process downloadProcess = Runtime.getRuntime().exec(downloadCmdArr);
      downloadProcess.waitFor();

      // Extract the azcopy executable from the tarball
      String extractCmd = EXTRACT_CMD + hadoopAzureDir.getAbsolutePath();
      String[] extractCmdArr = {AZCOPY_CMD_SHELL,
          AZCOPY_CMD_OPTION, extractCmd};
      Process extractProcess = Runtime.getRuntime().exec(extractCmdArr);
      extractProcess.waitFor();

      // Rename the azcopy_linux_amd64_* directory to 'azcopy' and move it to the hadoop-azure directory
      String renameCmd = MOVE_CMD + hadoopAzureDir.getAbsolutePath() + AZCOPY_DOWNLOADED_DIR_NAME + azcopyDirPath;
      String[] renameCmdArr = {AZCOPY_CMD_SHELL, AZCOPY_CMD_OPTION, renameCmd};
      Process renameProcess = Runtime.getRuntime().exec(renameCmdArr);
      renameProcess.waitFor();

      // Remove the downloaded tarball and azcopy folder
      String cleanupCmd = REMOVE_CMD + hadoopAzureDir.getAbsolutePath() + AZCOPY_DOWNLOADED_TAR_NAME;
      String[] cleanupCmdArr = {AZCOPY_CMD_SHELL, AZCOPY_CMD_OPTION, cleanupCmd};
      Process cleanupProcess = Runtime.getRuntime().exec(cleanupCmdArr);
      cleanupProcess.waitFor();

      // Set the execute permission on the azcopy executable
      String chmodCmd = CHMOD_CMD + azcopyDirPath;
      String[] chmodCmdArr = {AZCOPY_CMD_SHELL, AZCOPY_CMD_OPTION, chmodCmd};
      Process chmodProcess = Runtime.getRuntime().exec(chmodCmdArr);
      chmodProcess.waitFor();
    }
  }

  private boolean fileExists(String filePath) {
    File file = new File(filePath);
    return file.exists();
  }

  private void createShellScript(String scriptPath, String scriptContent) {
    RandomAccessFile file = null;
    FileLock fileLock = null;
    try {
      file = new RandomAccessFile(scriptPath, "rw");
      FileChannel fileChannel = file.getChannel();

      // Try acquiring the lock
      fileLock = fileChannel.tryLock();
      if (fileLock != null) {
        // Write to the file
        file.writeBytes(scriptContent);

        // Release the lock
        fileLock.release();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (fileLock != null && fileLock.isValid()) {
          fileLock.release();
        }
        if (file != null) {
          file.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void setExecutablePermission(String scriptPath) throws IOException, InterruptedException {
    String chmodCmd = CHMOD_CMD + scriptPath;
    String[] chmodCmdArr = {AZCOPY_CMD_SHELL, AZCOPY_CMD_OPTION, chmodCmd};
    Process chmodProcess = Runtime.getRuntime().exec(chmodCmdArr);
    chmodProcess.waitFor();
  }

  private void runShellScript(String scriptPath, String argument) throws Exception {
    try {
      ProcessBuilder pb = new ProcessBuilder(scriptPath, argument);
      Process p = pb.start();
      // wait for the process to finish
      p.waitFor();
    } catch (IOException e) {
      throw new IOException(e.getMessage());
    } catch (InterruptedException e) {
      throw new InterruptedException(e.getMessage());
    }
  }
}
