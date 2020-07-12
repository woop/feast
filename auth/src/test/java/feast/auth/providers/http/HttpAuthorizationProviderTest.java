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
package feast.auth.providers.http;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

import feast.auth.authorization.AuthorizationResult;
import feast.auth.providers.http.ketoadaptor.invoker.OpenAPI2SpringBoot;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.jupiter.api.TestInstance;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import sh.ory.keto.ApiClient;
import sh.ory.keto.ApiException;
import sh.ory.keto.Configuration;
import sh.ory.keto.api.EnginesApi;
import sh.ory.keto.model.OryAccessControlPolicy;
import sh.ory.keto.model.OryAccessControlPolicyRole;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpAuthorizationProviderTest {

  private static final String DEFAULT_FLAVOR = "glob";
  private static int KETO_PORT = 4466;
  private static int KETO_ADAPTOR_PORT = 45123;

  String project = "myproject";
  String subjectInProject = "good_member@example.com";
  String subjectNotInProject = "bad_member@example.com";
  String subjectIsAdmin = "bossman@example.com";
  String subjectClaim = "email";

  HttpAuthorizationProvider provider;

  @ClassRule
  public static DockerComposeContainer environment =
      new DockerComposeContainer(new File("src/test/resources/keto/docker-compose.yml"))
          .withExposedService("keto_1", KETO_PORT, forHttp("/health/ready").forStatusCode(200));

  @org.junit.jupiter.api.BeforeAll
  void setUp() throws Exception {
    // Start Keto with Docker Compose
    environment.start();
    String ketoExternalHost = environment.getServiceHost("keto_1", KETO_PORT);
    Integer ketoExternalPort = environment.getServicePort("keto_1", KETO_PORT);
    String ketoExternalUrl = String.format("http://%s:%s", ketoExternalHost, ketoExternalPort);

    // Seed Keto with data
    seedKeto(ketoExternalUrl);

    // Start Keto Adaptor server with Spring Boot
    String[] args =
        new String[] {"--server.port=" + KETO_ADAPTOR_PORT, "--keto.url=" + ketoExternalUrl};
    OpenAPI2SpringBoot.main(args);

    // Create HTTP Authorization provider that connects to Keto Adaptor
    Map<String, String> options = new HashMap<>();
    options.put("authorizationUrl", "http://localhost:" + KETO_ADAPTOR_PORT);
    provider = new HttpAuthorizationProvider(options);
  }

  @org.junit.jupiter.api.AfterAll
  void tearDown() {
    environment.stop();
  }

  @org.junit.jupiter.api.Test
  void checkAccessShouldPassForProjectMember() {
    Authentication authentication = getAuthentication(subjectClaim, subjectInProject);
    AuthorizationResult result = provider.checkAccessToProject(project, authentication);
    assertTrue(result.isAllowed());
  }

  @org.junit.jupiter.api.Test
  void checkAccessShouldFailForNonProjectMember() {
    Authentication authentication = getAuthentication(subjectClaim, subjectNotInProject);
    AuthorizationResult result = provider.checkAccessToProject(project, authentication);
    assertFalse(result.isAllowed());
  }

  @org.junit.jupiter.api.Test
  void checkAccessShouldPassForAdminEvenIfNotMember() {
    Authentication authentication = getAuthentication(subjectClaim, subjectIsAdmin);
    AuthorizationResult result = provider.checkAccessToProject(project, authentication);
    assertTrue(result.isAllowed());
  }

  /** Creates a fake Authentication object that contains the user identity as a claim */
  private Authentication getAuthentication(String subjectClaim, String subject) {
    Authentication authentication = mock(Authentication.class);
    Jwt principal = mock(Jwt.class);
    when(authentication.getPrincipal()).thenReturn(principal);
    Map<String, Object> claims = new HashMap<>();
    claims.put(subjectClaim, subject);
    when(principal.getClaims()).thenReturn(claims);
    return authentication;
  }

  private void seedKeto(String url) throws ApiException {
    ApiClient ketoClient = Configuration.getDefaultApiClient();
    ketoClient.setBasePath(url);
    EnginesApi enginesApi = new EnginesApi(ketoClient);

    // Add policies
    OryAccessControlPolicy adminPolicy = getAdminPolicy();
    enginesApi.upsertOryAccessControlPolicy(DEFAULT_FLAVOR, adminPolicy);

    OryAccessControlPolicy projectPolicy = getMyProjectMemberPolicy();
    enginesApi.upsertOryAccessControlPolicy(DEFAULT_FLAVOR, projectPolicy);

    // Add policy roles
    OryAccessControlPolicyRole adminPolicyRole = getAdminPolicyRole();
    enginesApi.upsertOryAccessControlPolicyRole(DEFAULT_FLAVOR, adminPolicyRole);

    OryAccessControlPolicyRole myProjectMemberPolicyRole = getMyProjectMemberPolicyRole();
    enginesApi.upsertOryAccessControlPolicyRole(DEFAULT_FLAVOR, myProjectMemberPolicyRole);
  }

  private OryAccessControlPolicyRole getMyProjectMemberPolicyRole() {
    OryAccessControlPolicyRole role = new OryAccessControlPolicyRole();
    role.setId(String.format("roles:%s-project-members", project));
    role.setMembers(Collections.singletonList("users:" + subjectInProject));
    return role;
  }

  private OryAccessControlPolicyRole getAdminPolicyRole() {
    OryAccessControlPolicyRole role = new OryAccessControlPolicyRole();
    role.setId("roles:admin");
    role.setMembers(Collections.singletonList("users:" + subjectIsAdmin));
    return role;
  }

  private OryAccessControlPolicy getAdminPolicy() {
    OryAccessControlPolicy policy = new OryAccessControlPolicy();
    policy.setId("policies:admin");
    policy.subjects(Collections.singletonList("roles:admin"));
    policy.resources(Collections.singletonList("resources:**"));
    policy.actions(Collections.singletonList("actions:**"));
    policy.effect("allow");
    policy.conditions(null);
    return policy;
  }

  private OryAccessControlPolicy getMyProjectMemberPolicy() {
    OryAccessControlPolicy policy = new OryAccessControlPolicy();
    policy.setId(String.format("policies:%s-project-members-policy", project));
    policy.subjects(Collections.singletonList(String.format("roles:%s-project-members", project)));
    policy.resources(
        Arrays.asList(
            String.format("resources:projects:%s", project),
            String.format("resources:projects:%s:**", project)));
    policy.actions(Collections.singletonList("actions:**"));
    policy.effect("allow");
    policy.conditions(null);
    return policy;
  }
}
