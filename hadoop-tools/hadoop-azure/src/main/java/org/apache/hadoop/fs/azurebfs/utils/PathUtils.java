package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

public class PathUtils {
  public static String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    String relPath = path.toUri().getPath();
    if (relPath.isEmpty()) {
      // This means that path passed by user is absolute path of root without "/" at end.
      relPath = ROOT_PATH;
    }
    return relPath;
  }
}
