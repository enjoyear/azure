package com.chen.guo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Credentials implements ICredentialProvider {
  public final static String CLIENT_PREFIX = "azure.sp.";
  public final static Properties credentialFile = loadCredentials();

  @Override
  public String getClientId(String clientName) {
    return credentialFile.getProperty(CLIENT_PREFIX + clientName + ".id");
  }

  @Override
  public String getClientSecret(String clientName) {
    return credentialFile.getProperty(CLIENT_PREFIX + clientName + ".secret");
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
}
