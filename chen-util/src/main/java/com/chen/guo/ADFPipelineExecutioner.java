package com.chen.guo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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
  private final ICredentialProvider _credentials;
  private final String _resourceGroupName;
  private final String _dataFactoryName;
  private final String _actionUriTemplate;
  private static final String apiVersion = "2018-06-01";

  public ADFPipelineExecutioner(ICredentialProvider credentials, String resourceGroupName, String dataFactoryName) {
    _credentials = credentials;
    _resourceGroupName = resourceGroupName;
    _dataFactoryName = dataFactoryName;
    String factoryManagementUri = String.format(
        "https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers/Microsoft.DataFactory/factories/%s",
        credentials.getSubscriptionId(), resourceGroupName, dataFactoryName);
    _actionUriTemplate = factoryManagementUri + "/%s" + String.format("?api-version=%s", apiVersion);
  }

  public static void main(String[] args) throws JsonProcessingException, MalformedURLException, ExecutionException, InterruptedException {
    ICredentialProvider credentials = new CredentialsFileProvider("/Users/chguo/Documents/credentials/azure_credentials.properties");
    AuthenticationHelper authHelper = new AuthenticationHelper(credentials);
    String sp = "adf-executor";

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

    String resourceGroupName = "demo";
    String datafactoryName = "Data-Factory-demo";

    ADFPipelineExecutioner executioner = new ADFPipelineExecutioner(credentials, resourceGroupName, datafactoryName);
    try {
      //HttpUriRequest request = executioner.requestPipelineExecution(bodyJson, token, "spark-only");
      //HttpUriRequest request = executioner.requestPipelineRunStatus(token, "24156418-335f-4337-a9f8-804ddb7274e9");
      HttpUriRequest request = executioner.requestPipelineActivityRuns(token, "3fac1e5f-fc27-4d53-84d6-da83b0426b5d");

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

  private HttpUriRequest requestPipelineExecution(String bodyJson, AuthenticationResult token, String pipelineName) throws URISyntaxException, IOException {
    URIBuilder pipelineExecutionBuilder = new URIBuilder(buildUri(String.format("pipelines/%s/createRun", pipelineName)));
    System.out.println("Built URI: " + pipelineExecutionBuilder.toString());
    URI uri = pipelineExecutionBuilder.build();
    return postRequest(bodyJson, token, uri);
  }

  /**
   * @param token
   * @param pipelineRunId this is the RunGroupId, or the runId for the pipeline, not the activity
   * @return
   * @throws URISyntaxException
   * @throws IOException
   */
  private HttpUriRequest requestPipelineRunStatus(AuthenticationResult token, String pipelineRunId) throws URISyntaxException, IOException {
    URIBuilder runMonitorBuilder = new URIBuilder(buildUri(String.format("pipelineruns/%s", pipelineRunId)));
    System.out.println("Built URI: " + runMonitorBuilder.toString());
    URI uri = runMonitorBuilder.build();
    return getRequest(token, uri);
  }

  private HttpUriRequest requestPipelineActivityRuns(AuthenticationResult token, String pipelineRunId) throws URISyntaxException, IOException {
    URIBuilder runMonitorBuilder = new URIBuilder(buildUri(String.format("pipelineruns/%s/queryActivityruns", pipelineRunId)));
    System.out.println("Built URI: " + runMonitorBuilder.toString());
    URI uri = runMonitorBuilder.build();
    return postRequest("", token, uri);
  }

  private HttpUriRequest postRequest(String bodyJson, AuthenticationResult token, URI uri) throws IOException {
    HttpPost request = new HttpPost(uri);
    request.setHeader("Content-Type", "application/json"); //request
    request.setHeader("Accept", "application/json"); //response
    request.setHeader("Authorization", "Bearer " + token.getAccessToken());
    // Request body
    System.out.println("Json Payload: " + bodyJson);
    StringEntity reqEntity = new StringEntity(bodyJson);
    request.setEntity(reqEntity);
    return request;
  }

  private HttpUriRequest getRequest(AuthenticationResult token, URI uri) throws IOException {
    HttpGet request = new HttpGet(uri);
    request.setHeader("Content-Type", "application/json"); //request
    request.setHeader("Accept", "application/json"); //response
    request.setHeader("Authorization", "Bearer " + token.getAccessToken());
    return request;
  }

  private String buildUri(String action) {
    return String.format(_actionUriTemplate, action);
  }
}