package feast.auth.providers.http.ketoadaptor.api;


import feast.auth.providers.http.ketoadaptor.model.AuthorizationResult;
import lombok.Getter;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.api.HealthApi;
import sh.ory.keto.model.HealthStatus;

@Getter
public class KetoAuth {

  // Client to connect to actual Keto server
  ApiClient ketoClient;
  String DEFAULT_FLAVOR = "glob";

  public KetoAuth(String url) {
    ketoClient = Configuration.getDefaultApiClient();
    ketoClient.setBasePath(url);

    // Test connectivity to Keto
    try {
      getHealthzApi().isInstanceReady();
    } catch (ApiException e) {
     throw new RuntimeException("Could not connect to Keto server.");
    }
  }

  public EnginesApi getEnginesApi(){
    return new EnginesApi(this.getKetoClient());
  }

  public HealthApi getHealthzApi(){
    return new HealthApi(this.getKetoClient());
  }

  public AuthorizationResult checkAccess(String action, String resource, String subject, Object context) {
    AuthorizationResult result = new AuthorizationResult();
    result.setAllowed(true);
    return result;
  }
}
