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
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;

import static org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest.SHORTENED_GUID_LEN;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;

/**
 * Singleton class to create a file or folder in Azure Blob Storage using Azcopy tool.
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

  private static final String USER_DIR_SYSTEM_PROPERTY = "user.dir";
  private static final String HADOOP_AZURE_DIR = "hadoop-azure";
  private static final String AZCOPY_DIR_NAME = "azcopy";
  private static final String AZCOPY_EXECUTABLE_NAME = "azcopy";
  private static final String FILE_CREATION_SCRIPT_NAME = "createAzcopyFile";
  private static final String FOLDER_CREATION_SCRIPT_NAME = "createAzcopyFolder";
  private static final String DIR_NOT_FOUND_ERROR = " directory not found";
  private static final String AZCOPY_DOWNLOADED_DIR_NAME = "/azcopy_linux_amd64_*/* ";
  private static final String AZCOPY_DOWNLOADED_TAR_NAME = "/azcopy_linux_amd64_* azcopy.tar.gz";

  private static final String AZCOPY_CMD_SHELL = "bash";
  private static final String AZCOPY_CMD_OPTION = "-c";
  private static final String AZCOPY_DOWNLOAD_URL = "https://aka.ms/downloadazcopy-v10-linux";
  private static final String AZCOPY_DOWNLOAD_CMD = "wget " + AZCOPY_DOWNLOAD_URL + " -O azcopy.tar.gz" + " --no-check-certificate";
  private static final String EXTRACT_CMD = "tar -xf azcopy.tar.gz -C ";
  private static final String MOVE_CMD = "mv ";
  private static final String REMOVE_CMD = "rm -rf ";
  private static final String CHMOD_CMD = "chmod +x ";
  private static final char QUESTION_MARK = '?';
  private static final ReentrantLock LOCK = new ReentrantLock();

  // Same azcopy tool can be used for all the tests running in save JVM.
  private static AzcopyToolHelper azcopyToolHelper = null;

  private AzcopyToolHelper() {

  }

  /**
   * Constructor to initialize the AzcopyToolHelper.
   * Azcopy tool work with SAS based authentication. SAS can be configured using
   * test configuration "fs.azure.test.fixed.sas.token".
   * @param sasToken to be used for authentication.
   */
  public static AzcopyToolHelper getInstance(String sasToken) throws IOException, InterruptedException {
    if (azcopyToolHelper == null) {
      LOCK.lock();
      try {
        if (azcopyToolHelper == null) {
          azcopyToolHelper = new AzcopyToolHelper();
          azcopyToolHelper.init(sasToken);
        }
      } finally {
        LOCK.unlock();
      }
    }
    return azcopyToolHelper;
  }

  private void init(String sasToken) throws IOException, InterruptedException {
    this.sasToken = sasToken.charAt(0) == QUESTION_MARK ? sasToken : QUESTION_MARK + sasToken;
    this.hadoopAzureDir = findHadoopAzureDir();
    this.azcopyDirPath = hadoopAzureDir.getAbsolutePath() + FORWARD_SLASH + AZCOPY_DIR_NAME;
    this.azcopyExecutablePath = azcopyDirPath + FORWARD_SLASH + AZCOPY_EXECUTABLE_NAME;

    // Synchronized on directory creation.
    // If multiple process try to create directory, only one will succeed and others will return.
    downloadAzcopyExecutableIfNotPresent();

    // Change working directory to the hadoop-azure directory.
    System.setProperty(USER_DIR_SYSTEM_PROPERTY, hadoopAzureDir.getAbsolutePath());

    // Create shell scripts for file creation if not exists in synchronized manner.
    fileCreationScriptPath = azcopyDirPath + FORWARD_SLASH + FILE_CREATION_SCRIPT_NAME
        + StringUtils.right(UUID.randomUUID().toString(), SHORTENED_GUID_LEN) + ".sh";
    String fileCreationScriptContent = "blobPath=$1\n" + "echo $blobPath\n"
        + azcopyExecutablePath + " copy \"" + azcopyDirPath
        + "/NOTICE.txt\" $blobPath\n";
    createShellScript(fileCreationScriptPath, fileCreationScriptContent);

    // Create shell scripts for folder creation if not exists in synchronized manner.
    folderCreationScriptPath = azcopyDirPath + FORWARD_SLASH + FOLDER_CREATION_SCRIPT_NAME
        + StringUtils.right(UUID.randomUUID().toString(), SHORTENED_GUID_LEN) + ".sh";
    String folderCreationScriptContent = "blobPath=$1\n" + "echo $blobPath\n"
        + azcopyExecutablePath + " copy \"" + azcopyDirPath
        + "\" $blobPath --recursive\n";
    createShellScript(folderCreationScriptPath, folderCreationScriptContent);
  }

  /**
   * Create a file with implicit parent in the container using Azcopy tool.
   * @param absolutePathToBeCreated absolute path to be created.
   * @throws Exception if file creation fails.
   */
  public void createFileUsingAzcopy(String absolutePathToBeCreated) throws Exception {
    if (absolutePathToBeCreated != null) {
      absolutePathToBeCreated = absolutePathToBeCreated.replace(
          ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME) + sasToken;
      runShellScript(fileCreationScriptPath, absolutePathToBeCreated);
    }
  }

  /**
   * Create a implicit folder with implicit parent in the container using Azcopy tool.
   * @param absolutePathToBeCreated absolute path to be created.
   * @throws Exception
   */
  public void createFolderUsingAzcopy(String absolutePathToBeCreated) throws Exception {
    if (absolutePathToBeCreated != null) {
      absolutePathToBeCreated = absolutePathToBeCreated.replace(
          ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME) + sasToken;
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

  // Synchronized method to download azcopy executable if not present.
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

  /**
   * Create a shell script if not already created.
   * @param scriptPath to be created
   * @param scriptContent to be written in the script.
   */
  private void createShellScript(String scriptPath, String scriptContent) {
    File scriptFile = new File(scriptPath);
    if (!scriptFile.exists()) {
      try {
        FileWriter writer = new FileWriter(scriptFile);
        writer.write(scriptContent);
        writer.close();
        scriptFile.setExecutable(true); // make the script executable
      } catch (IOException e) {
        System.out.println("Error creating shell script: " + e.getMessage());
      }
    }
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
