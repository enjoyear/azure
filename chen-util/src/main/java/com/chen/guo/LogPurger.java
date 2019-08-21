package com.chen.guo;

import com.chen.guo.storage.BlobBasics;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class LogPurger {
  public static void main(String[] args) throws InvalidKeyException, StorageException, URISyntaxException, IOException {
    String storageAccountConnectionString = "";
    CloudBlobContainer container = new BlobBasics(storageAccountConnectionString).getBlobContainer("sparkjobs");
    Iterable<ListBlobItem> rootDirs = container.listBlobs();
    for (ListBlobItem rootDir : rootDirs) {
      String rootPath = rootDir.getUri().getPath();
      //URI: https://abs0execution.blob.core.windows.net/sparkjobs/3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
      //Path: /sparkjobs/3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
      //Prefix: 3d3d8d23-d45d-4c51-b55f-a1778b7ed920/
      System.out.println(rootDir.getClass().getSimpleName() + ": " + rootPath);
      if (rootDir instanceof CloudBlobDirectory) {
        CloudBlobDirectory dir = (CloudBlobDirectory) rootDir;
        Iterable<ListBlobItem> blobs = container.listBlobs(dir.getPrefix());
        for (ListBlobItem blob : blobs) {
          System.out.println(blob.getClass().getSimpleName() + ": " + blob.getUri().getPath());
        }
      }
      CloudBlockBlob blockRef = container.getBlockBlobReference(rootPath);
      //System.out.println(blockRef);
    }
  }
}
