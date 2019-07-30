package com.chen.guo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Doc:
 * https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-rest-api
 */
public class ADFPipelineExecutioner {

  public static void main(String[] args) throws JsonProcessingException, MalformedURLException, ExecutionException, InterruptedException {
    ICredentialProvider credentials = new CredentialsFileProvider("/Users/chguo/Documents/credentials/azure_credentials.properties");
    AuthenticationHelper authHelper = new AuthenticationHelper(credentials);
    String sp = "real-adf-manager";

    Map<String, String> body = new HashMap<>();
    body.put("SasToken", "R32-PQ2n7L7Kv_-B8VpwWCtAu3FkX_QHZ6-2cgY4eZg");
    String bodyJson = new ObjectMapper().writeValueAsString(body);

    ExecutorService service = Executors.newCachedThreadPool();
    AuthenticationContext authContext = new AuthenticationContext(authHelper.getAuthorityUri(), false, service);
    AuthenticationResult token = authContext.acquireToken(AuthenticationHelper.managementResourceUri, credentials.getClientCredential(sp), AuthenticationHelper.authenticationCallback).get();
    System.out.println(token.getAccessToken());
    System.out.println(token.getExpiresOnDate());
    System.out.println(token.getUserInfo());
    System.out.println(token.getRefreshToken());

    HttpClient httpclient = HttpClients.createDefault();

    try {
      URIBuilder builder = new URIBuilder(String.format("https://management.azure.com/subscriptions/%s/resourceGroups/demo/providers/Microsoft.DataFactory/factories/Data-Factory-demo/pipelines/demo/createRun?api-version=2018-06-01", credentials.getSubscriptionId()));
      URI uri = builder.build();
      HttpPost request = new HttpPost(uri);
      request.setHeader("Content-Type", "application/json"); //request
      request.setHeader("Accept", "application/json"); //response
      request.setHeader("Authorization", "Bearer " + token.getAccessToken());
      // Request body
      System.out.println(bodyJson);
      StringEntity reqEntity = new StringEntity(bodyJson);
      request.setEntity(reqEntity);

      HttpResponse response = httpclient.execute(request);
      HttpEntity entity = response.getEntity();

      if (entity != null) {
        System.out.println(EntityUtils.toString(entity));
      }
      System.out.println("Done");
      service.shutdown(); //service needs to be shutdown, otherwise it won't exit
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
}