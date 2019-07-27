package com.chen.guo.security;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class KeyVaultADALAuthenticator {

  public static void main(String[] args) {
    String authorization = "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3";
    String resourceUri = "https://management.core.windows.net/";

    KeyVaultClient kvClient = new KeyVaultClient(createCredentials());
  }

  /**
   * Creates a new KeyVaultCredential based on the access token obtained.
   */
  private static ServiceClientCredentials createCredentials() {
    return new KeyVaultCredentials() {

      //Callback that supplies the token type and access token on request.
      @Override
      public String doAuthenticate(String authorization, String resource, String scope) {

        AuthenticationResult authResult;
        try {
          authResult = getAccessToken(authorization, resource);
          return authResult.getAccessToken();
        } catch (Exception e) {
          e.printStackTrace();
        }
        return "";
      }

    };
  }

  /**
   * Private helper method that gets the access token for the authorization and resource depending on which variables are supplied in the environment.
   */
  private static AuthenticationResult getAccessToken(String authorization, String resource) throws InterruptedException, ExecutionException, MalformedURLException {
    String clientId = System.getProperty("AZURE_CLIENT_ID");
    String clientKey = System.getProperty("AZURE_CLIENT_SECRET");

    AuthenticationResult result;
    ExecutorService service = null;
    try {
      service = Executors.newFixedThreadPool(1);
      AuthenticationContext context = new AuthenticationContext(authorization, false, service);

      ClientCredential credentials = new ClientCredential(clientId, clientKey);
      Future<AuthenticationResult> future = context.acquireToken(resource, credentials, null);

      result = future.get();
    } finally {
      service.shutdown();
    }

    if (result == null) {
      throw new RuntimeException("Authentication results were null.");
    }
    return result;
  }
}