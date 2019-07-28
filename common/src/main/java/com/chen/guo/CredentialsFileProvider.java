package com.chen.guo;

import com.microsoft.aad.adal4j.ClientCredential;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CredentialsFileProvider implements ICredentialProvider {
  public final static String CLIENT_PREFIX = "azure.sp.";
  public final static Properties credentialFile = loadCredentials();

  @Override
  public ClientCredential getClientCredential(String clientName) {
    return new ClientCredential(getClientId(clientName), getClientSecret(clientName));
  }

  @Override
  public String getADId() {
    return credentialFile.getProperty("azure.ad.id");
  }

  @Override
  public String getSubscriptionId() {
    return credentialFile.getProperty("azure.subscription.id");
  }

  public static Properties loadCredentials() {
    try (InputStream input = new FileInputStream("/Users/chguo/Documents/credentials/azure_credentials.properties")) {

      Properties prop = new Properties();
      prop.load(input);
      return prop;
    } catch (IOException ex) {
      ex.printStackTrace();
      return null;
    }
  }

  private String getClientId(String clientName) {
    return credentialFile.getProperty(CLIENT_PREFIX + clientName + ".id");
  }

  private String getClientSecret(String clientName) {
    return credentialFile.getProperty(CLIENT_PREFIX + clientName + ".secret");
  }
}
