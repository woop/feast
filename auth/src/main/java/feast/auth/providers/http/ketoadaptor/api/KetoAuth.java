/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.auth.providers.http.ketoadaptor.api;

import java.util.HashMap;
import lombok.Getter;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.api.HealthApi;
import sh.ory.keto.model.AuthorizationResult;
import sh.ory.keto.model.OryAccessControlPolicyAllowedInput;

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

  public EnginesApi getEnginesApi() {
    return new EnginesApi(this.getKetoClient());
  }

  public HealthApi getHealthzApi() {
    return new HealthApi(this.getKetoClient());
  }

  public AuthorizationResult checkAccess(
      String action, String resource, String subject, Object context) throws ApiException {
    EnginesApi engine = getEnginesApi();
    OryAccessControlPolicyAllowedInput input = new OryAccessControlPolicyAllowedInput();
    input.setAction("actions:" + action);
    input.setResource("resources:" + resource);
    input.setSubject("users:" + subject);
    input.setContext(new HashMap<String, Object>());
    sh.ory.keto.model.AuthorizationResult ketoResult =
        engine.doOryAccessControlPoliciesAllow(DEFAULT_FLAVOR, input);
    return ketoResult;
  }
}
