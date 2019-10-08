package com.chen.guo;

import com.chen.guo.adf.RunFilterParameters;
import com.chen.guo.auth.AuthenticationHelper;
import com.chen.guo.auth.CredentialsFileProvider;
import com.chen.guo.auth.ICredentialProvider;
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
  private static final String apiVersion20180601 = "?api-version=2018-06-01";
  private static final String apiVersion20180601Preview = "?api-version=2018-06-01-preview";
  private final String _hdinsightTemplate;

  public ADFPipelineExecutioner(ICredentialProvider credentials, String resourceGroupName, String dataFactoryName) {
    _credentials = credentials;
    _resourceGroupName = resourceGroupName;
    _dataFactoryName = dataFactoryName;
    String providesPrefix = String.format("https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers", credentials.getSubscriptionId(), resourceGroupName);


    String factoryManagementUri = String.format(providesPrefix + "/Microsoft.DataFactory/factories/%s", dataFactoryName);
    _actionUriTemplate = factoryManagementUri + "/%s" + apiVersion20180601;

    _hdinsightTemplate = providesPrefix + "/Microsoft.HDInsight/clusters/%s" + apiVersion20180601Preview;
  }

  public static void main(String[] args) throws JsonProcessingException, MalformedURLException, ExecutionException, InterruptedException {
    ICredentialProvider credentials = new CredentialsFileProvider("/Users/chguo/Documents/credentials/azure_credentials.properties");
    AuthenticationHelper authHelper = new AuthenticationHelper(credentials);
    String sp = "adf-executor";

    ExecutorService service = Executors.newCachedThreadPool();
    AuthenticationContext authContext = new AuthenticationContext(authHelper.getAuthorityUri(), false, service);
    AuthenticationResult token = authContext.acquireToken(AuthenticationHelper.managementResourceUri, credentials.getClientCredential(sp), AuthenticationHelper.authenticationCallback).get();
    System.out.println("****** Acquired Token ******");
    System.out.println("Access Token: " + token.getAccessToken());
    System.out.println("Token Expiration" + token.getExpiresOnDate());
    System.out.println("Token User Info" + token.getUserInfo());
    System.out.println("Refresh Token" + token.getRefreshToken());
    System.out.println("****** Acquired Token ******");
    System.out.println("****** ****** ****** ****** ");

    HttpClient httpclient = HttpClients.createDefault();

    String resourceGroupName = "demo";
    String datafactoryName = "Data-Factory-demo";

    ADFPipelineExecutioner executioner = new ADFPipelineExecutioner(credentials, resourceGroupName, datafactoryName);
    try {
      HttpUriRequest request = executioner.executePipeline(token, "spark-only3");
      //HttpUriRequest request = executioner.getPipelineRunStatus(token, "24156418-335f-4337-a9f8-804ddb7274e9");
      //HttpUriRequest request = executioner.getPipelineActivityRuns(token, "ddb03ce9-b70e-444f-ac83-be72379eff11");
      //HttpUriRequest request = executioner.requestHDInsightInfo(token, "s8c7205f8-081e-49c6-a4fa-3d8528be2c0e");
      //HttpUriRequest request = executioner.queryPipelineExecutions(token);

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

  /**
   * execute a pipeline with JSON input
   */
  private HttpUriRequest executePipeline(AuthenticationResult token, String pipelineName) throws URISyntaxException, IOException {
    Map<String, String> body = new HashMap<>();
    body.put("FsCustomer", "hi");
    //body.put("CustomerSecret", "{\"type\":\"SecureString\", \"value\":\"why so stupid\"}");

    URIBuilder pipelineExecutionBuilder = new URIBuilder(buildPipelineActionUri(String.format("pipelines/%s/createRun", pipelineName)));
    System.out.println("Built URI: " + pipelineExecutionBuilder.toString());
    URI uri = pipelineExecutionBuilder.build();
    return postRequest(new ObjectMapper().writeValueAsString(body), token, uri);
  }

  /**
   * @param token
   * @param pipelineRunId this is the RunGroupId, or the runId for the pipeline, not the activity
   * @return
   * @throws URISyntaxException
   * @throws IOException
   */
  private HttpUriRequest getPipelineRunStatus(AuthenticationResult token, String pipelineRunId) throws URISyntaxException, IOException {
    URIBuilder runMonitorBuilder = new URIBuilder(buildPipelineActionUri(String.format("pipelineruns/%s", pipelineRunId)));
    System.out.println("Built URI: " + runMonitorBuilder.toString());
    URI uri = runMonitorBuilder.build();
    return getRequest(token, uri);
  }

  /**
   * get activity runs for a specific pipeline run
   * https://docs.microsoft.com/en-us/rest/api/datafactory/activityruns/querybypipelinerun
   */
  private HttpUriRequest getPipelineActivityRuns(AuthenticationResult token, String pipelineRunId) throws URISyntaxException, IOException {
    URIBuilder runMonitorBuilder = new URIBuilder(buildPipelineActionUri(String.format("pipelineruns/%s/queryActivityruns", pipelineRunId)));
    System.out.println("Built URI: " + runMonitorBuilder.toString());
    URI uri = runMonitorBuilder.build();
    return postRequest("", token, uri);
  }

  /**
   * get details of a hdinsight cluster
   */
  private HttpUriRequest requestHDInsightInfo(AuthenticationResult token, String hdinsightName) throws URISyntaxException, IOException {
    URIBuilder runMonitorBuilder = new URIBuilder(String.format(_hdinsightTemplate, hdinsightName));
    System.out.println("Built URI: " + runMonitorBuilder.toString());
    URI uri = runMonitorBuilder.build();
    return getRequest(token, uri);
  }

  /**
   * query pipeline runs based on filters
   * https://docs.microsoft.com/en-us/rest/api/datafactory/pipelineruns/querybyfactory
   * https://docs.microsoft.com/en-us/azure/data-factory/monitor-programmatically#data-range
   */
  private HttpUriRequest queryPipelineExecutions(AuthenticationResult token) throws URISyntaxException, IOException {
    RunFilterParameters parameters = new RunFilterParameters();

    parameters.setLastUpdatedAfter("2019-08-20T00:00:00.0000000Z");
    parameters.setLastUpdatedBefore("2019-08-21T23:49:48.3686473Z");

    RunFilterParameters.RunQueryFilter filter = new RunFilterParameters.RunQueryFilter(
        RunFilterParameters.RunQueryFilter.RunQueryFilterOperand.PipelineName,
        RunFilterParameters.RunQueryFilter.RunQueryFilterOperator.Equals,
        new String[]{"spark-only"});
    parameters.setFilters(new RunFilterParameters.RunQueryFilter[]{filter});
    String filterJson = parameters.toString();
    System.out.println(filterJson);

    URIBuilder pipelineExecutionBuilder = new URIBuilder(buildPipelineActionUri("queryPipelineRuns"));
    System.out.println("Built URI: " + pipelineExecutionBuilder.toString());
    URI uri = pipelineExecutionBuilder.build();
    return postRequest(filterJson, token, uri);
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

  //The template url is
  // https://management.azure.com/subscriptions/id/resourceGroups/rg/providers/Microsoft.DataFactory/factories/name/<xxx>?api-version=2018-06-01
  private String buildPipelineActionUri(String action) {
    return String.format(_actionUriTemplate, action);
  }
}