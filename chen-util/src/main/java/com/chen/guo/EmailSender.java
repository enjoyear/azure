package com.chen.guo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.*;
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
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Example:
 * https://contos.io/working-with-identity-in-an-azure-function-1a981e10b900?gi=af7c9ad2c8d1
 * https://blogs.msdn.microsoft.com/stuartleeks/2018/02/19/azure-functions-and-app-service-authentication/
 * https://blogs.msdn.microsoft.com/ben/2018/11/07/client-app-calling-azure-function-with-aad/
 * <p>
 * Youtube:
 * https://www.youtube.com/watch?v=N5I59z3qY0A
 * <p>
 * Trouble Shooting:
 * https://stackoverflow.com/questions/50213999/in-azure-logic-app-i-am-getting-directapiauthorizationrequired/52874244#52874244
 * https://www.bruttin.com/2017/06/16/secure-logicapp-with-apim.html
 */
public class EmailSender {

  @FunctionName("echo")
  public static String echo(
      @HttpTrigger(name = "req", methods = {HttpMethod.PUT}, authLevel = AuthorizationLevel.ANONYMOUS, route = "items/{id}") String inputReq,
      @TableInput(name = "item", tableName = "items", partitionKey = "Example", rowKey = "{id}", connection = "AzureWebJobsStorage") TestInputData inputData,
      @TableOutput(name = "myOutputTable", tableName = "Person", connection = "AzureWebJobsStorage") OutputBinding<Person> testOutputData
  ) {
    testOutputData.setValue(new Person("Partition", "Row", "Name"));
    return "Hello, " + inputReq + " and " + inputData.getKey() + ".";
  }

  public static class TestInputData {
    public String getKey() {
      return this.RowKey;
    }

    private String RowKey;
  }

  public static class Person {
    public String PartitionKey;
    public String RowKey;
    public String Name;

    public Person(String p, String r, String n) {
      this.PartitionKey = p;
      this.RowKey = r;
      this.Name = n;
    }
  }

  public static void main(String[] args) throws JsonProcessingException, MalformedURLException, ExecutionException, InterruptedException {
    Map<String, String> body = new HashMap<>();
    body.put("pipeline_run_id", Integer.toString(new Random().nextInt()));
    body.put("name", "name:" + Integer.toString(new Random().nextInt()));
    body.put("status", "suc");
    String bodyJson = new ObjectMapper().writeValueAsString(body);

    ICredentialProvider credentials = new CredentialsFileProvider("/Users/chguo/Documents/credentials/azure_credentials.properties");
    AuthenticationHelper authHelper = new AuthenticationHelper(credentials);
    String sp = "email-notification-lambda";

    ExecutorService service = Executors.newCachedThreadPool();
    AuthenticationContext authContext = new AuthenticationContext(authHelper.getAuthorityUri(), false, service);
    //!!! This is called the App ID URI in the legacy view !!!
    String resourceUri = "https://email-notification-lambda.azurewebsites.net";
    AuthenticationResult token = authContext.acquireToken(resourceUri, credentials.getClientCredential(sp), AuthenticationHelper.authenticationCallback).get();
    System.out.println(token.getAccessToken());
    System.out.println(token.getExpiresOnDate());
    System.out.println(token.getUserInfo());
    System.out.println(token.getRefreshToken());

    HttpClient httpclient = HttpClients.createDefault();

    try {
      //URIBuilder builder = new URIBuilder("https://email-notification-lambda.azurewebsites.net/chen/email/send");
      URIBuilder builder = new URIBuilder("https://email-notification-lambda.azurewebsites.net/api/HttpTrigger1");
      URI uri = builder.build();
      HttpPost request = new HttpPost(uri);
      request.setHeader("Content-Type", "application/json");
      //request.setHeader("X-Sas-Token", "R32-PQ2n7L7Kv_-B8VpwWCtAu3FkX_QHZ6-2cgY4eZg");
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