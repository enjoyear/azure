package com.chen.guo.auth.security;

import com.chen.guo.auth.AuthenticationHelper;
import com.chen.guo.auth.CredentialsFileProvider;
import com.chen.guo.auth.ICredentialProvider;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.azure.keyvault.models.SecretBundle;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class KeyVaultADALAuthenticator {
  public static void main(String[] args) {
    ICredentialProvider credentials = new CredentialsFileProvider("/Users/chguo/Documents/credentials/azure_credentials.properties");
    AuthenticationHelper authHelper = new AuthenticationHelper(credentials);
    String sp = "akv-reader"; //this sp must be granted access in the KV's Access Policies
    String vaultURL = "https://chen-vault.vault.azure.net/";

    KeyVaultClient kvClient = new KeyVaultClient(createCredentials(credentials, sp));
    SecretBundle secret = kvClient.getSecret(vaultURL, "sas-token");
    System.out.println(secret.value());
  }

  /**
   * Creates a new KeyVaultCredential based on the access token obtained.
   */
  public static ServiceClientCredentials createCredentials(ICredentialProvider credentialProvider, String sp) {

    return new KeyVaultCredentials() {
      /**
       * Callback that supplies the token type and access token on request.
       */
      @Override
      public String doAuthenticate(String authorization, String resource, String scope) {
        System.out.println("Authorization URL: " + authorization);
        System.out.println("Resource: " + resource);
        System.out.println("Scope: " + scope);
        System.out.println("Performing OAuth Authentication for " + sp);

        AuthenticationResult result;
        ExecutorService service = null;
        try {
          service = Executors.newFixedThreadPool(1);
          AuthenticationContext context = new AuthenticationContext(authorization, false, service);
          //Read the SP's id and secret from credential file
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