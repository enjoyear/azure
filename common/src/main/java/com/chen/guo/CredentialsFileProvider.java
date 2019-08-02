package com.chen.guo;

import com.chen.guo.security.KeyVaultADALAuthenticator;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.models.SecretBundle;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

public class CredentialsFileProvider implements ICredentialProvider {
  public final static String CLIENT_PREFIX = "azure.sp.";
  private final Properties _credentialFile;

  public CredentialsFileProvider(String propertiesFile) {
    _credentialFile = loadCredentials(propertiesFile);
  }

  public CredentialsFileProvider(Reader content) {
    _credentialFile = loadCredentials(content);
  }

  @Override
  public ClientCredential getClientCredential(String clientName) {
    String clientId = getClientId(clientName);
    String clientSecret = getClientSecret(clientName);
    System.out.println(String.format("Read credential file: id %s, secret %s for client %s", clientId, clientSecret, clientName));
    return new ClientCredential(clientId, clientSecret);
  }

  @Override
  public String getADId() {
    return _credentialFile.getProperty("azure.ad.id");
  }

  @Override
  public String getSubscriptionId() {
    return _credentialFile.getProperty("azure.subscription.id");
  }

  public Properties loadCredentials(String propertiesFile) {
    try (InputStream input = new FileInputStream(propertiesFile)) {

      Properties prop = new Properties();
      prop.load(input);
      return prop;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public Properties loadCredentials(Reader content) {
    try {
      Properties prop = new Properties();
      prop.load(content);
      return prop;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private String getClientId(String clientName) {
    return _credentialFile.getProperty(CLIENT_PREFIX + clientName + ".id");
  }

  private String getClientSecret(String clientName) {
    return _credentialFile.getProperty(CLIENT_PREFIX + clientName + ".secret");
  }

  /**
   * Instead of passing the storageAccountConnectionString,
   * the akv-reader's id and secret need to be passed in in production.
   */
  public static String getSecretFromSA(String storageAccountConnectionString, String secretName) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
    System.out.println(String.format("Storage Account Connection String: %s", storageAccountConnectionString));
    CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageAccountConnectionString);
    CloudBlobClient cloudBlobClient = storageAccount.createCloudBlobClient();
    CloudBlobContainer container = cloudBlobClient.getContainerReference("demo-jars");
    CloudBlockBlob blockRef = container.getBlockBlobReference("properties/azure_credentials.properties");
    String s = blockRef.downloadText();
    System.out.println("Loaded credential file blob content: " + s);
    ICredentialProvider credentials = new CredentialsFileProvider(new StringReader(s));
    String sp = "akv-reader"; //this sp must be granted access in the KV's Access Policies
    ServiceClientCredentials akvReaderCredential = KeyVaultADALAuthenticator.createCredentials(credentials, sp);
    KeyVaultClient kvClient = new KeyVaultClient(akvReaderCredential);

    String vaultURL = "https://chen-vault.vault.azure.net/";
    SecretBundle secret = kvClient.getSecret(vaultURL, secretName);
    System.out.println(String.format("Fetched secret %s: %s", secretName, secret.value()));
    return secret.value();
  }
}
