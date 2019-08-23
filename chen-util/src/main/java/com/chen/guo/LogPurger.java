package com.chen.guo;

import com.chen.guo.storage.BlobBasics;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Date;
import java.util.Iterator;

/**
 * See examples here
 * https://github.com/Azure-Samples/storage-blob-java-getting-started/blob/master/src/BlobBasics.java
 * <p>
 * Create back-up container
 * azcopy cp "https://abs0execution.blob.core.windows.net/sparkjobs/*" "https://abs0execution.blob.core.windows.net/sparkjobs-copy/" --recursive=true
 * azcopy cp "https://abs0execution.blob.core.windows.net/sparkjobs/*" "https://abs0execution.blob.core.windows.net/sparkjobs-copy/" --recursive=true
 */
@Slf4j
public class LogPurger {
  public static void main(String[] args) throws InvalidKeyException, StorageException, URISyntaxException {
    DateTime jodaDate = new DateTime(2019, 8, 21, 17, 20);
    Date date = jodaDate.toDate();
    String saConnectionString = args[0];
    LogPurger logPurger = new LogPurger(saConnectionString, "sparkjobs", date);
    logPurger.printBlobs();
  }

  private final String _storageAccountConnectionString;
  private final Date _purgeDate;
  private final CloudBlobContainer _container;

  /**
   * @param saConnectionString
   * @param purgeDate          logs older than purgeDate will be purged
   */
  public LogPurger(String saConnectionString, String containerName, Date purgeDate) throws InvalidKeyException, StorageException, URISyntaxException {
    _storageAccountConnectionString = saConnectionString;
    _purgeDate = purgeDate;
    _container = new BlobBasics(_storageAccountConnectionString).getBlobContainer(containerName);
  }

  public void printBlobs() throws URISyntaxException, StorageException {
    long start = System.currentTimeMillis();
    int count = flatScan();
    //int count = dfsScan(container);
    log.info(String.format("Take %.1f seconds to get a total of %s blobs", (System.currentTimeMillis() - start) / 1000.0, count));
  }

  /**
   * Scan the container in FlatBlobListing mode
   *
   * @return the number of blobs
   */
  private int flatScan() throws URISyntaxException, StorageException {
    int count = 0;
    Iterable<ListBlobItem> blobs = _container.listBlobs("", true);
    Iterator<ListBlobItem> iterator = blobs.iterator();
    while (iterator.hasNext()) {
      ++count;
      long start = System.currentTimeMillis();
      blobPrint(iterator.next());
      log.info(String.format("list time: %s", System.currentTimeMillis() - start));
    }
    return count;
  }

  /**
   * Scan the container in FlatBlobListing mode
   *
   * @return the number of dirs and blobs
   */
  private int dfsScan() throws URISyntaxException, StorageException {
    int[] count = new int[]{0};
    Iterable<ListBlobItem> rootDirs = _container.listBlobs();
    for (ListBlobItem rootDir : rootDirs) {
      dfsTraverseBlobItem(rootDir, count);
    }
    return count[0];
  }

  /**
   * Field value examples:
   * URI: https://abs0execution.blob.core.windows.net/sparkjobs/3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
   * Path: /sparkjobs/3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
   * Prefix: 3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
   *
   * @param item the entry item for the traversal
   */
  private void dfsTraverseBlobItem(ListBlobItem item, int[] count) throws URISyntaxException, StorageException {
    ++count[0];

    if (item instanceof CloudBlobDirectory) {
      CloudBlobDirectory dir = (CloudBlobDirectory) item;
//      log.info(String.format("%s: %s", dir.getPrefix(), dir.getUri().getPath()));
      Iterable<ListBlobItem> blobs = _container.listBlobs(dir.getPrefix());
//      List<ListBlobItem> b = StreamSupport.stream(blobs.spliterator(), false).collect(Collectors.toList());

      for (ListBlobItem blobItem : blobs) {
        dfsTraverseBlobItem(blobItem, count);
      }
    } else {
      if (item instanceof CloudBlockBlob) {
        blobPrint(item);
      } else {
        throw new RuntimeException("What is this? " + item.getClass().getSimpleName() + ": " + item.getUri().getPath());
      }
    }
  }

  private void blobPrint(ListBlobItem item) throws URISyntaxException, StorageException {
    CloudBlockBlob blob = (CloudBlockBlob) item;
    BlobProperties properties = blob.getProperties();
    Date lastModified = properties.getLastModified();
    //String path = item.getUri().getPath();
    String path = blob.getName();

    if (lastModified.before(_purgeDate)) {
      log.info(String.format("[Deleting]%s: %s", path, lastModified));
      CloudBlockBlob blockRef = _container.getBlockBlobReference(path);
      long start = System.currentTimeMillis();
      blockRef.delete();
      log.info(String.format("delete time: %s", System.currentTimeMillis() - start));
    } else {
      log.info(String.format("[Keep]%s: %s", path, lastModified));
    }
  }
}
