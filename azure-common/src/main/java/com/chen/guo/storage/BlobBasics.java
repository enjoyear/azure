package com.chen.guo.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

/**
 * https://github.com/Azure-Samples/storage-blob-java-getting-started/blob/master/src/BlobBasics.java
 */
public class BlobBasics {
  private final String _storageAccountConnectionString;

  public BlobBasics(String storageAccountConnectionString) {
    _storageAccountConnectionString = storageAccountConnectionString;
  }

  public CloudBlobContainer getBlobContainer(String containerName) throws URISyntaxException, InvalidKeyException, StorageException {
    CloudStorageAccount storageAccount = CloudStorageAccount.parse(_storageAccountConnectionString);
    CloudBlobClient cloudBlobClient = storageAccount.createCloudBlobClient();
    return cloudBlobClient.getContainerReference(containerName);
  }
}
