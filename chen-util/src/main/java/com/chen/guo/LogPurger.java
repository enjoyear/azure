package com.chen.guo;

import com.chen.guo.storage.BlobBasics;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

@Slf4j
public class LogPurger {
  public static void main(String[] args) throws InvalidKeyException, StorageException, URISyntaxException, IOException {
    LogPurger logPurger = new LogPurger("as");
    logPurger.printBlobs("sparkjobs");
  }

  private final String _storageAccountConnectionString;

  public LogPurger(String saConnectionString) {
    _storageAccountConnectionString = saConnectionString;

  }

  public void printBlobs(String containerName) throws InvalidKeyException, StorageException, URISyntaxException {
    CloudBlobContainer container = getContainer(containerName);
    //Get the flat list
    Iterable<ListBlobItem> rootDirs = container.listBlobs("", true);
    //Iterable<ListBlobItem> rootDirs = container.listBlobs();

    int[] count = new int[]{0};
    for (ListBlobItem rootDir : rootDirs) {
      dfsTraverseBlobItem(container, rootDir, count);
      //CloudBlockBlob blockRef = container.getBlockBlobReference(rootPath);
      //System.out.println(blockRef);
    }
    log.info(String.format("Get a total of %s blobs(folders and files)", count[0]));
  }

  public CloudBlobContainer getContainer(String containerName) throws InvalidKeyException, StorageException, URISyntaxException {
    return new BlobBasics(_storageAccountConnectionString).getBlobContainer(containerName);
  }

  /**
   * Field value examples:
   * URI: https://abs0execution.blob.core.windows.net/sparkjobs/3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
   * Path: /sparkjobs/3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
   * Prefix: 3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
   *
   * @param item the entry item for the traversal
   */
  private void dfsTraverseBlobItem(CloudBlobContainer container, ListBlobItem item, int[] count) {
    ++count[0];

    if (item instanceof CloudBlobDirectory) {
      CloudBlobDirectory dir = (CloudBlobDirectory) item;
//      log.info(String.format("%s: %s", dir.getPrefix(), dir.getUri().getPath()));
      Iterable<ListBlobItem> blobs = container.listBlobs(dir.getPrefix());
//      List<ListBlobItem> b = StreamSupport.stream(blobs.spliterator(), false).collect(Collectors.toList());

      for (ListBlobItem blobItem : blobs) {
        dfsTraverseBlobItem(container, blobItem, count);
      }
    } else {
      if (item instanceof CloudBlockBlob) {
        CloudBlockBlob blob = (CloudBlockBlob) item;
        BlobProperties properties = blob.getProperties();
        log.info(String.format("%s: %s", item.getUri().getPath(), properties.getLastModified()));
      } else {
        throw new RuntimeException("What is this? " + item.getClass().getSimpleName() + ": " + item.getUri().getPath());
      }
    }
  }
}
