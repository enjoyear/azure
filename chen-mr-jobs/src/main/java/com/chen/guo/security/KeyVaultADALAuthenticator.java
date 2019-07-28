package com.chen.guo.security;

import com.chen.guo.AuthenticationHelper;
import com.chen.guo.CredentialsFileProvider;
import com.chen.guo.ICredentialProvider;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class KeyVaultADALAuthenticator {
  public static void main(String[] args) {
    ICredentialProvider credentials = new CredentialsFileProvider();
    AuthenticationHelper authHelper = new AuthenticationHelper(credentials);
    String sp = "key-vault-manager";
    String vaultURL = "https://chen-vault.vault.azure.net/";

    KeyVaultClient kvClient = new KeyVaultClient(createCredentials(credentials, sp));
    kvClient.getSecret(vaultURL, "sas-token");
  }

  /**
   * Creates a new KeyVaultCredential based on the access token obtained.
   */
  private static ServiceClientCredentials createCredentials(ICredentialProvider credentialProvider, String sp) {

    return new KeyVaultCredentials() {
      /**
       * Callback that supplies the token type and access token on request.
       */
      @Override
      public String doAuthenticate(String authorization, String resource, String scope) {
        System.out.println("Authorization URL: " + authorization);
        System.out.println("Resource: " + resource);
        System.out.println("Scope: " + scope);
        AuthenticationResult result;
        ExecutorService service = null;
        try {
          service = Executors.newFixedThreadPool(1);
          AuthenticationContext context = new AuthenticationContext(authorization, false, service);
          ClientCredential credentials = credentialProvider.getClientCredential(sp);
          Future<AuthenticationResult> future = context.acquireToken(resource, credentials, AuthenticationHelper.authenticationCallback);
          result = future.get();
        } catch (Exception e) {
          //InterruptedException, ExecutionException, MalformedURLException
          throw new RuntimeException(e);
        } finally {
          service.shutdown();
        }
        System.out.println("Access Token: " + result.getAccessToken());
        assert result != null : "Authentication results were null";
        return result.getAccessToken();
      }
    };
  }
}