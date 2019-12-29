package com.chen.guo.auth;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import com.chen.guo.auth.security.KeyVaultADALAuthenticator;
import com.chen.guo.storage.BlobBasics;
import com.chen.guo.util.PropertyFileLoader;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.models.SecretBundle;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import lombok.Getter;


public class CredentialsFileProvider implements ICredentialProvider {
  public final static String CLIENT_PREFIX = "azure.sp.";
  @Getter
  private final Properties _credentialFile;

  public CredentialsFileProvider(String propertiesFile) {
    _credentialFile = PropertyFileLoader.load(propertiesFile);
  }

  public CredentialsFileProvider(Reader content) {
    _credentialFile = loadCredentials(content);
  }

  @Override
  public ClientCredential getClientCredential(String clientName) {
    String clientId = getClientId(clientName);
    String clientSecret = getClientSecret(clientName);
    System.out.println(
        String.format("Read credential file: id %s, secret %s for client %s", clientId, clientSecret, clientName));
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
  public static String getSecretFromSA(String storageAccountConnectionString, String secretName)
      throws URISyntaxException, InvalidKeyException, StorageException, IOException {
    System.out.println(String.format("Storage Account Connection String: %s", storageAccountConnectionString));
    CloudBlobContainer container = new BlobBasics(storageAccountConnectionString).getBlobContainer("demo-jars");
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
