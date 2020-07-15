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

import static org.springframework.http.HttpStatus.*;

import feast.auth.providers.http.ketoadaptor.configuration.KetoProperties;
import feast.auth.providers.http.ketoadaptor.model.AuthorizationResult;
import feast.auth.providers.http.ketoadaptor.model.CheckAccessRequest;
import feast.auth.providers.http.ketoadaptor.model.InlineResponse500;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Optional;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.request.NativeWebRequest;
import sh.ory.keto.ApiException;

@javax.annotation.Generated(
    value = "org.openapitools.codegen.languages.SpringCodegen",
    date = "2020-07-12T11:35:52.408245+08:00[Asia/Singapore]")
@Controller
@RequestMapping("${openapi.feastAuthorizationServer.base-path:}")
public class CheckAccessApiController implements CheckAccessApi {

  private final NativeWebRequest request;
  Logger logger = LoggerFactory.getLogger(CheckAccessApiController.class);

  @Autowired KetoProperties ketoProperties;

  @org.springframework.beans.factory.annotation.Autowired
  public CheckAccessApiController(NativeWebRequest request) {
    this.request = request;
  }

  @Override
  public Optional<NativeWebRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  /**
   * POST /checkAccess : Check whether request is authorized to access a specific resource
   *
   * @param body Request containing user, resource, and action information. Used to make an
   *     authorization decision. (required)
   * @return Authorization passed response (status code 200) or Authorization failed response
   *     (status code 403) or The standard error format (status code 500)
   */
  @Override
  @ApiOperation(
      value = "Check whether request is authorized to access a specific resource",
      nickname = "checkAccessPost",
      notes = "",
      response = AuthorizationResult.class,
      tags = {})
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 200,
            message = "Authorization passed response",
            response = AuthorizationResult.class),
        @ApiResponse(
            code = 403,
            message = "Authorization failed response",
            response = AuthorizationResult.class),
        @ApiResponse(
            code = 500,
            message = "The standard error format",
            response = InlineResponse500.class)
      })
  @RequestMapping(
      value = "/checkAccess",
      produces = {"application/json"},
      consumes = {"application/json"},
      method = RequestMethod.POST)
  public ResponseEntity<AuthorizationResult> checkAccessPost(
      @ApiParam(
              value =
                  "Request containing user, resource, and action information. Used to make an authorization decision.",
              required = true)
          @Valid
          @RequestBody
          CheckAccessRequest body) {
    KetoAuth ketoAuth = getKetoAuth();
    try {
      sh.ory.keto.model.AuthorizationResult ketoResult =
          ketoAuth.checkAccess(
              body.getAction(), body.getResource(), body.getSubject(), body.getContext());
      return ResponseEntity.ok(new AuthorizationResult().allowed(ketoResult.getAllowed()));
    } catch (ApiException e) {
      logger.error(e.getMessage());
      return new ResponseEntity<AuthorizationResult>(
          new AuthorizationResult().allowed(false), HttpStatus.valueOf(e.getCode()));
    }
  }

  @Bean
  public KetoAuth getKetoAuth() {
    return new KetoAuth(ketoProperties.getUrl());
  }
}
