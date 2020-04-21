package com.chen.guo.adf;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.chen.guo.auth.AuthenticationHelper;
import com.chen.guo.auth.CredentialsFileProvider;
import com.chen.guo.auth.ICredentialProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;


public class AdfIRCreator {
  private final ICredentialProvider _credentials;
  private final String _resourceGroupName;
  private final String _dataFactoryName;
  private final String _actionUriTemplate;
  private static final String apiVersion20180601 = "?api-version=2018-06-01";
  private static final String apiVersion20180601Preview = "?api-version=2018-06-01-preview";
  private final String _hdinsightTemplate;

  public AdfIRCreator(ICredentialProvider credentials, String resourceGroupName, String dataFactoryName) {
    _credentials = credentials;
    _resourceGroupName = resourceGroupName;
    _dataFactoryName = dataFactoryName;
    String providesPrefix = String.format("https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers",
        credentials.getSubscriptionId(), resourceGroupName);

    String factoryManagementUri =
        String.format(providesPrefix + "/Microsoft.DataFactory/factories/%s", dataFactoryName);
    _actionUriTemplate = factoryManagementUri + "/%s" + apiVersion20180601;

    _hdinsightTemplate = providesPrefix + "/Microsoft.HDInsight/clusters/%s" + apiVersion20180601Preview;
  }

  public static void main(String[] args)
      throws JsonProcessingException, MalformedURLException, ExecutionException, InterruptedException {
    final UUID uuid = UUID.randomUUID();
    System.out.println(uuid);
    System.exit(0);

    ICredentialProvider credentials =
        new CredentialsFileProvider("/Users/chguo/Documents/credentials/azure_credentials_ei.properties");
    AuthenticationHelper authHelper = new AuthenticationHelper(credentials);
    String sp = "spi-cdppoc-ei";

    ExecutorService service = Executors.newCachedThreadPool();
    AuthenticationContext authContext = new AuthenticationContext(authHelper.getAuthorityUri(), false, service);
    AuthenticationResult token = authContext
        .acquireToken(AuthenticationHelper.managementResourceUri, credentials.getClientCredential(sp),
            AuthenticationHelper.authenticationCallback).get();
    System.out.println("****** Acquired Token ******");
    System.out.println("Access Token: " + token.getAccessToken());
    System.out.println("Token Expiration" + token.getExpiresOnDate());
    System.out.println("Token User Info" + token.getUserInfo());
    System.out.println("Refresh Token" + token.getRefreshToken());
    System.out.println("****** Acquired Token ******");
    System.out.println("****** ****** ****** ****** ");

    HttpClient httpclient = HttpClients.createDefault();

    String resourceGroupName = "conflation-cdpxxx-ei-uscs-rg";
    String datafactoryName = "liktwoconfcdpxxxeuscsdf";

    AdfIRCreator creator = new AdfIRCreator(credentials, resourceGroupName, datafactoryName);
    try {
      HttpUriRequest request = creator.createIR(token, "reserved-ir");
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
  private HttpUriRequest createIR(AuthenticationResult token, String irName)
      throws URISyntaxException, IOException {
    String json =
        new StringBuilder().append("{\n").append("    \"properties\": {\n").append("        \"type\": \"Managed\",\n")
            .append("        \"typeProperties\": {\n").append("            \"computeProperties\": {\n")
            .append("                \"location\": \"southcentralus\",\n").append("                \"virtualNetwork\":\n")
            .append("                                        {\n").append(
            "                                               \"SubnetId\":\"/subscriptions/c3b5358d-d824-450c-badc-734462b9bbf1/resourceGroups/conflation-cdpxxx-ei-uscs-rg/providers/Microsoft.Network/virtualNetworks/cdp-adf-vnet/subnets/cdp-ir-snet\"\n")
            .append("                                        }\n").append("                                    }\n")
            .append("        }\n").append("    }\n").append("}").toString();
    URIBuilder pipelineExecutionBuilder =
        new URIBuilder(buildPipelineActionUri(String.format("integrationRuntimes/%s", irName)));
    System.out.println("Built URI: " + pipelineExecutionBuilder.toString());
    URI uri = pipelineExecutionBuilder.build();
    return putRequest(json, token, uri);
  }

  private HttpUriRequest putRequest(String bodyJson, AuthenticationResult token, URI uri)
      throws IOException {
    HttpPut request = new HttpPut(uri);
    request.setHeader("Content-Type", "application/json"); //request
    request.setHeader("Accept", "application/json"); //response
    final String clientRequestId =
        ((CredentialsFileProvider) _credentials).get_credentialFile().getProperty("x-ms-client-request-id");
    request.setHeader("x-ms-client-request-id", clientRequestId);
    request.setHeader("Authorization", "Bearer " + token.getAccessToken());
    // Request body
    System.out.println("Json Payload: " + bodyJson);
    StringEntity reqEntity = new StringEntity(bodyJson);
    request.setEntity(reqEntity);
    return request;
  }

  //The template url is
  // https://management.azure.com/subscriptions/id/resourceGroups/rg/providers/Microsoft.DataFactory/factories/name/<xxx>?api-version=2018-06-01
  private String buildPipelineActionUri(String action) {
    return String.format(_actionUriTemplate, action);
  }
}