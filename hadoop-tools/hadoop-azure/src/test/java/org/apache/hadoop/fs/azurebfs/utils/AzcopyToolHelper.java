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

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;

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
  private String accountName;
  private String fileSystemName;
  private String sasToken;

  private static final String USER_DIR_SYSTEM_PROPERTY = "user.dir";
  private static final String HADOOP_AZURE_DIR = "hadoop-azure";
  private static final String AZCOPY_DIR_NAME = "azcopy";
  private static final String AZCOPY_TOOL_FILE_NAME = "azcopy";
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

  /**
   * Azcopy tool work with SAS based authentication. SAS can be configured using
   * test configuration "fs.azure.test.fixed.sas.token".
   * @param accountName on which tool as has to run.
   * @param fileSystemName container inside which implicit path has to be created.
   * @param sasToken to be used by azcopy tool for authentication.
   */
  public AzcopyToolHelper(String accountName, String fileSystemName, String sasToken) {
    this.accountName = accountName.replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX);
    this.fileSystemName = fileSystemName;
    this.sasToken = sasToken;
  }

  /**
   * Create a file in the container using Azcopy tool.
   * @param pathFromContainerRoot absolute path to be created.
   * @throws Exception
   */
  public void createFileUsingAzcopy(String pathFromContainerRoot) throws Exception {
    // Add the path you want to copy to as config.
    if (pathFromContainerRoot != null) {
      createFileOrFolder(pathFromContainerRoot, true);
    }
  }

  /**
   * Create a folder in the container using Azcopy tool.
   * @param pathFromContainerRoot absolute path to be created.
   * @throws Exception
   */
  public void createFolderUsingAzcopy(String pathFromContainerRoot) throws Exception {
    // Add the path you want to copy to as config.
    if (pathFromContainerRoot != null) {
      createFileOrFolder(pathFromContainerRoot, false);
    }
  }

  private void downloadAzcopyExecutableIfNotPresent() throws IOException, InterruptedException {
    // Find the hadoop-azure directory from the current working directory.
    File currentDir = new File(System.getProperty(USER_DIR_SYSTEM_PROPERTY));
    if (!currentDir.isDirectory() && !currentDir.getName().equals(HADOOP_AZURE_DIR)) {
      hadoopAzureDir = findHadoopAzureDir(currentDir);
      if (hadoopAzureDir == null) {
        throw new FileNotFoundException(HADOOP_AZURE_DIR + DIR_NOT_FOUND_ERROR);
      }
    } else {
      hadoopAzureDir = currentDir;
    }

    // Check if azcopy directory is present in the hadoop-azure directory.
    // If not present, create a directory and download azcopy tool.
    azcopyDirPath = hadoopAzureDir.getAbsolutePath() + FORWARD_SLASH + AZCOPY_DIR_NAME;
    File azcopyDir = new File(azcopyDirPath);
    if (!azcopyDir.exists()) {
      azcopyDir.mkdir();
      // Check if azcopy tool is present in the azcopy directory.
      String azcopyPath = azcopyDirPath + FORWARD_SLASH + AZCOPY_TOOL_FILE_NAME;
      File azcopyFile = new File(azcopyPath);
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
    // Change working directory to the hadoop-azure directory
    System.setProperty(USER_DIR_SYSTEM_PROPERTY, hadoopAzureDir.getAbsolutePath());
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

  private void createFileOrFolder(String pathFromContainerRoot, boolean isFile) throws Exception {
    downloadAzcopyExecutableIfNotPresent();
    String url = "https://" + accountName + FORWARD_SLASH + fileSystemName + FORWARD_SLASH +
        pathFromContainerRoot;

    if (isFile) {
      createFileCreationScript(azcopyDirPath, "createFile" + Thread.currentThread().getName() + ".sh", azcopyDirPath, sasToken, url);
    } else {
      createFolderCreationScript(azcopyDirPath, "createFolder" + Thread.currentThread().getName() + ".sh", azcopyDirPath, sasToken, url);
    }

    String path;
    if (isFile) {
      path = azcopyDirPath + "/createFile" + Thread.currentThread().getName() + ".sh";
    } else {
      path = azcopyDirPath + "/createFolder" + Thread.currentThread().getName() + ".sh";
    }
    try {
      ProcessBuilder pb = new ProcessBuilder(path);
      Process p = pb.start();
      // wait for the process to finish
      int exitCode = p.waitFor();
    } catch (IOException e) {
      throw new IOException(e.getMessage());
    } catch (InterruptedException e) {
      throw new InterruptedException(e.getMessage());
    }
    String cleanupCmd = REMOVE_CMD + path;
    String[] cleanupCmdArr = {AZCOPY_CMD_SHELL, AZCOPY_CMD_OPTION, cleanupCmd};
    Process cleanupProcess = Runtime.getRuntime().exec(cleanupCmdArr);
    cleanupProcess.waitFor();
  }

  private static void createFileCreationScript(String folderPath, String scriptName, String azcopyPath, String sasToken, String containerName) {
    String blobPath = containerName + "?" + sasToken; // construct the blob path
    String scriptContent = "blobPath=\"" + blobPath + "\"\n"
        + "echo $blobPath\n"
        + azcopyPath + "/azcopy copy \"" + azcopyPath + "/NOTICE.txt\" $blobPath\n"; // construct the script content
    File scriptFile = new File(folderPath, scriptName);
    try {
      FileWriter writer = new FileWriter(scriptFile);
      writer.write(scriptContent);
      writer.close();
      boolean written = scriptFile.setExecutable(true); // make the script executable
      System.out.println("Script created at " + scriptFile.getAbsolutePath());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void createFolderCreationScript(String folderPath, String scriptName, String azcopyPath, String sasToken, String containerName) {
    String blobPath = containerName + "?" + sasToken; // construct the blob path
    String scriptContent = "blobPath=\"" + blobPath + "\"\n"
        + "echo $blobPath\n"
        + azcopyPath + "/azcopy copy \"" + azcopyPath + "\" $blobPath --recursive\n"; // construct the script content
    File scriptFile = new File(folderPath, scriptName);
    try {
      FileWriter writer = new FileWriter(scriptFile);
      writer.write(scriptContent);
      writer.close();
      boolean written = scriptFile.setExecutable(true); // make the script executable
      System.out.println("Script created at " + scriptFile.getAbsolutePath());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}