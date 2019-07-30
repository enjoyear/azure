package com.chen.guo;

import com.microsoft.aad.adal4j.ClientCredential;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
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
    return new ClientCredential(getClientId(clientName), getClientSecret(clientName));
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
}
