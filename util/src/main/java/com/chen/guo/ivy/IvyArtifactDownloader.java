package com.chen.guo.ivy;

import groovy.grape.Grape;
import groovy.lang.GroovyClassLoader;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.extern.slf4j.Slf4j;


/**
 * A utility class that provides the capabilities of
 * 1. download ivy artifacts using Groovy Grape
 * 2. unzip zipped ivy artifact to a temporary location
 */
@Slf4j
public class IvyArtifactDownloader {
  /**
   * The buffer size of the buffer converting from a zipInputStream to a fileOutputStream
   */
  private static final int BUFFER_SIZE = 4096;
  /**
   * The @see <a href="https://ant.apache.org/ivy/history/latest-milestone/settings.html">ivy setting file</a>
   */
  private static final String IVY_SETTINGS_FILE_NAME = "ivysettings.xml";

  private IvyArtifactDownloader() {
  }

  public static void main(String[] args) throws Exception {
//    IvyArtifactDownloader.downloadArtifactZipAndUnzip(
//        new ArtifactCoordinate("com.linkedin.chguo-spark-job", "workflow1-azkaban", "0.0.5", "adfazureei1", "zip"));
    IvyArtifactDownloader.downloadArtifactZipAndUnzip(
        new ArtifactCoordinate("com.linkedin.chguo-spark-job", "chguo-spark-job-buildspec", "0.0.5", "", "json"));
  }

  /**
   * Download the zip artifact given ivy meta data and then unzip it to a temporary location
   * @param artifactCoordinate the artifact to be downloaded
   * @return the path where the artifact zip file is unzipped to
   * @throws IOException when downloading failed or unzipping failed
   */
  public static Path downloadArtifactZipAndUnzip(ArtifactCoordinate artifactCoordinate) throws IOException {
    File artifactZipFile = downloadArtifactZip(artifactCoordinate);
    final Path unzipPath = Files.createTempDirectory("chguo");
    unzipFile(artifactZipFile, unzipPath.toFile());
    return unzipPath;
  }

  /**
   * Download the zip artifact given ivy meta data.
   * The download destination is determined by Grape's cache implementation.
   * @param artifactCoordinate the artifact to be downloaded
   * @return the downloaded zip file, e.g. ~/.groovy/grapes/<org_name>/<module_name>
   * @throws IOException when downloading failed
   */
  public static File downloadArtifactZip(ArtifactCoordinate artifactCoordinate) throws IOException {
    Map<String, Object> artifactMap = new HashMap<>();
    artifactMap.putAll(artifactCoordinate.toMap());
    artifactMap.put("transitive", false);

    log.info("Downloading the zip file...");
    URI[] artifact = downloadArtifact(artifactMap);
    if (artifact.length != 1) {
      throw new IOException("Zip download failed.");
    }
    log.info("Downloaded the zip file at: " + artifact[0].toURL());
    return new File(artifact[0]);
  }

  /**
   * Download the Zip file based on the artifact coordinates
   * @param coordinatesMap a map specifying the artifact coordinates including
   *                    "org"(group), "module"(artifact), "version", "classifier", "ext"
   * @return the URI of the downloaded file
   * @throws IOException if the ivysettings.xml file cannot be found
   */
  private static URI[] downloadArtifact(final Map<String, Object> coordinatesMap) throws IOException {
    //Load ivy settings file from resources folder
    URL ivySettings = IvyArtifactDownloader.class.getClassLoader().getResource(IVY_SETTINGS_FILE_NAME);
    if (ivySettings == null) {
      throw new IOException(String.format("Failed to find %s from resources folder", IVY_SETTINGS_FILE_NAME));
    }
    String ivySettingFile = ivySettings.getFile();
    log.info("Setting for grape.config: " + ivySettingFile);
    printIvySettingsFile();
    System.setProperty("grape.config", ivySettingFile);

    Map<String, Object> args = new HashMap<>();
    args.put("classLoader", AccessController.doPrivileged(new PrivilegedAction<GroovyClassLoader>() {
      public GroovyClassLoader run() {
        return new GroovyClassLoader();
      }
    }));
    log.info("Got artifact coordinate map: " + coordinatesMap);
    return Grape.resolve(args, new Map[]{coordinatesMap});
  }

  /**
   * Print the IvySetting file to log
   */
  private static void printIvySettingsFile() throws IOException {
    try (InputStream inputStream = IvyArtifactDownloader.class.getClassLoader()
        .getResourceAsStream(IVY_SETTINGS_FILE_NAME)) {
      InputStreamReader isReader = new InputStreamReader(inputStream);
      //Creating a BufferedReader object
      BufferedReader reader = new BufferedReader(isReader);
      StringBuilder sb = new StringBuilder();
      String str;
      while ((str = reader.readLine()) != null) {
        sb.append(str).append(System.lineSeparator());
      }
      log.info(sb.toString());
    }
  }

  /**
   * Extracts a zip file to a directory, which will be created if not exist
   * @param zipFile the file to be unzipped
   * @param unzipPath the destination directory where the file will be unzipped to
   * @throws IOException if failed to unzip the file
   */
  public static void unzipFile(File zipFile, File unzipPath) throws IOException {
    //Create the destination directory if not exist
    if (!unzipPath.exists()) {
      unzipPath.mkdir();
    }

    try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile))) {
      ZipEntry entry = zipInputStream.getNextEntry();
      // iterates over entries in the zip file
      while (entry != null) {
        String filePath = unzipPath + File.separator + entry.getName();
        if (entry.isDirectory()) {
          // if the entry is a directory, make the directory
          File dir = new File(filePath);
          dir.mkdir();
        } else {
          // if the entry is a file, extracts it
          extractFile(zipInputStream, filePath);
        }
        zipInputStream.closeEntry();
        entry = zipInputStream.getNextEntry();
      }
    }

    log.info("Zip file is unzipped at : " + unzipPath);
  }

  /**
   * Extracts a zip file entry
   * @param zipInputStream the zipped file input stream
   * @param filePath the path where the a file entry will be unzipped to
   * @throws IOException if fail to write the zipInputStream into a file at specified filePath
   */
  private static void extractFile(ZipInputStream zipInputStream, String filePath) throws IOException {
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
      byte[] bytesIn = new byte[BUFFER_SIZE];
      int read;
      while ((read = zipInputStream.read(bytesIn)) != -1) {
        bos.write(bytesIn, 0, read);
      }
    }
  }
}
