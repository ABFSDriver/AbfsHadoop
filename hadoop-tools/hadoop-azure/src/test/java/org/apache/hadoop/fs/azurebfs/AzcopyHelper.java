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

package org.apache.hadoop.fs.azurebfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Assume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_FIXED_TOKEN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;

public class AzcopyHelper {

  private File hadoopAzureDir;
  private String azcopyDirPath;
  private String accountName;
  private String fileSystemName;
  private String sasToken;

  private static final String USER_DIR_SYSTEM_PROPERTY = "user.dir";
  private static final String HADOOP_AZURE_DIR = "hadoop-azure";
  private static final String AZCOPY_DIR = "azcopy";
  private static final String AZCOPY_EXECUTABLE = "azcopy";

  public AzcopyHelper(String accountName, String fileSystemName, String sasToken) throws Exception {
    this.accountName = accountName.replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX);
    this.fileSystemName = fileSystemName;
    this.sasToken = sasToken;
  }

  public void downloadAzcopyExecutableIfNotPresent() throws IOException, InterruptedException {
    // Find the hadoop-azure directory from the current working directory
    File currentDir = new File(System.getProperty(USER_DIR_SYSTEM_PROPERTY));
    if (!currentDir.isDirectory() && !currentDir.getName().equals(HADOOP_AZURE_DIR)) {
      hadoopAzureDir = findHadoopAzureDir(currentDir);
      if (hadoopAzureDir == null) {
        throw new FileNotFoundException(HADOOP_AZURE_DIR + " directory not found");
      }
    } else {
      hadoopAzureDir = currentDir;
    }

    // Check if azcopy directory is present in the hadoop-azure directory, create it if it doesn't exist
    azcopyDirPath = hadoopAzureDir.getAbsolutePath() + FORWARD_SLASH + AZCOPY_DIR;
    File azcopyDir = new File(azcopyDirPath);
    if (!azcopyDir.exists()) {
      boolean created = azcopyDir.mkdir();
      // Check if azcopy is present in the azcopy directory
      String azcopyPath = azcopyDirPath + FORWARD_SLASH + AZCOPY_EXECUTABLE;
      File azcopyFile = new File(azcopyPath);
      if (!azcopyFile.exists()) {
        // If azcopy is not present, download and extract it
        String downloadUrl = "https://aka.ms/downloadazcopy-v10-linux";
        String downloadCmd = "wget " + downloadUrl + " -O azcopy.tar.gz" + " --no-check-certificate";
        String[] downloadCmdArr = {"bash", "-c", downloadCmd};
        Process downloadProcess = Runtime.getRuntime().exec(downloadCmdArr);
        downloadProcess.waitFor();

        // Extract the azcopy executable from the tarball
        String extractCmd = "tar -xf azcopy.tar.gz -C " + hadoopAzureDir.getAbsolutePath();
        String[] extractCmdArr = {"bash", "-c", extractCmd};
        Process extractProcess = Runtime.getRuntime().exec(extractCmdArr);
        extractProcess.waitFor();

        // Rename the azcopy_linux_amd64_* directory to 'azcopy' and move it to the hadoop-azure directory
        String renameCmd = "mv " + hadoopAzureDir.getAbsolutePath() + "/azcopy_linux_amd64_*/* " + azcopyDirPath;
        String[] renameCmdArr = {"bash", "-c", renameCmd};
        Process renameProcess = Runtime.getRuntime().exec(renameCmdArr);
        renameProcess.waitFor();

        // Remove the downloaded tarball and azcopy folder
        String cleanupCmd = "rm -rf " + hadoopAzureDir.getAbsolutePath() + "/azcopy_linux_amd64_* azcopy.tar.gz";
        String[] cleanupCmdArr = {"bash", "-c", cleanupCmd};
        Process cleanupProcess = Runtime.getRuntime().exec(cleanupCmdArr);
        cleanupProcess.waitFor();

        // Set the execute permission on the azcopy executable
        String chmodCmd = "chmod +x " + azcopyDirPath;
        String[] chmodCmdArr = {"bash", "-c", chmodCmd};
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
      if (file.isDirectory() && file.getName().equals("hadoop-azure")) {
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

  public void createFileOrFolder(String pathFromContainerRoot, boolean isFile) throws Exception {
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
    String cleanupCmd = "rm -rf " + path;
    String[] cleanupCmdArr = {"bash", "-c", cleanupCmd};
    Process cleanupProcess = Runtime.getRuntime().exec(cleanupCmdArr);
    cleanupProcess.waitFor();
  }

  public void createFileUsingAzcopy(String pathFromContainerRoot) throws Exception {
    // Add the path you want to copy to as config.
    if (pathFromContainerRoot != null) {
      createFileOrFolder(pathFromContainerRoot, true);
    }
  }

  public void createFolderUsingAzcopy(String pathFromContainerRoot) throws Exception {
    // Add the path you want to copy to as config.
    if (pathFromContainerRoot != null) {
      createFileOrFolder(pathFromContainerRoot, false);
    }
  }

  public static void createFileCreationScript(String folderPath, String scriptName, String azcopyPath, String sasToken, String containerName) {
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

  public static void createFolderCreationScript(String folderPath, String scriptName, String azcopyPath, String sasToken, String containerName) {
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