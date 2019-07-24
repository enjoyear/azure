package com.chen.guo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.aad.adal4j.AuthenticationCallback;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
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
import java.util.concurrent.Executors;

/**
 * Doc:
 * https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-rest-api
 */
public class ADFPipelineExecutioner {

  public static void main(String[] args) throws JsonProcessingException, MalformedURLException, ExecutionException, InterruptedException {
    Map<String, String> body = new HashMap<>();
    body.put("SasToken", "R32-PQ2n7L7Kv_-B8VpwWCtAu3FkX_QHZ6-2cgY4eZg");
    String bodyJson = new ObjectMapper().writeValueAsString(body);

    String authorityUri = "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3"; //tenant id/aad id
    AuthenticationContext authContext = new AuthenticationContext(authorityUri, false, Executors.newCachedThreadPool());

    AuthenticationCallback callback = new AuthenticationCallback() {
      @Override
      public void onSuccess(Object result) {
        System.out.println("Success: " + result.toString());
      }

      @Override
      public void onFailure(Throwable exc) {
        System.out.println("Failed: " + exc.toString());
      }
    };
    String resourceUri = "https://management.core.windows.net/";
    String clientId = "556c6dd4-1d40-405b-9f34-b79751ba71ef";
    String clientSecret = "]Sj+c70/93FunFG?oApv2_o7c[3S47HP";
    AuthenticationResult token = authContext.acquireToken(resourceUri, new ClientCredential(clientId, clientSecret), callback).get();
    System.out.println(token.getAccessToken());
    System.out.println(token.getExpiresOnDate());
    System.out.println(token.getUserInfo());
    System.out.println(token.getRefreshToken());

    HttpClient httpclient = HttpClients.createDefault();

    try {
      URIBuilder builder = new URIBuilder("https://management.azure.com/subscriptions/d3f099cb-50ca-4bc7-9b48-5df8ea28757f/resourceGroups/demo/providers/Microsoft.DataFactory/factories/Data-Factory-demo/pipelines/demo/createRun?api-version=2018-06-01");
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

    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
}