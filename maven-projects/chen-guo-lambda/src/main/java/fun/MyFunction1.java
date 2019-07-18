package fun;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.util.Optional;

/**
 * Tutorial: https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-java-maven
 */
public class MyFunction1 {
  /**
   * This function listens at endpoint "/api/hello". Two ways to invoke it using "curl" command in bash:
   * 1. curl -d "HTTP Body" {your host}/api/hello
   * 2. curl {your host}/api/hello?name=HTTP%20Query
   */
  @FunctionName("hello")
  public HttpResponseMessage run(
      @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
      final ExecutionContext context) {
    context.getLogger().info("Java HTTP trigger processed a request.");

    // Parse query parameter
    String query = request.getQueryParameters().get("name");
    String name = request.getBody().orElse(query);

    if (name == null) {
      return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a name on the query string or in the request body").build();
    } else {
      return request.createResponseBuilder(HttpStatus.OK).body("Hello, " + name).build();
    }
  }
}
