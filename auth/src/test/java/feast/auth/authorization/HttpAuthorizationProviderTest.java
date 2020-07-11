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
package feast.auth.authorization;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;

class HttpAuthorizationProviderTest {

  HttpAuthorizationProvider provider;

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
    Map<String, String> options = new HashMap<>();
    options.put("authorizationUrl", "http://localhost");
    provider = new HttpAuthorizationProvider(options);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() {}

  @org.junit.jupiter.api.Test
  void checkAccessToProject() {
    String subject = "me@example.com";
    String subjectClaim = "email";
    String project = "my-project";
    Authentication authentication = getAuthentication(subjectClaim, subject);
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
}
