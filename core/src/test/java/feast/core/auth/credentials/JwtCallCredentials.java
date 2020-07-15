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
package feast.core.auth.credentials;

import com.nimbusds.jose.JOSEException;
import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.concurrent.Executor;

public final class JwtCallCredentials implements CallCredentials {

  private String jwt;

  public JwtCallCredentials(String jwt) throws JOSEException {
    this.jwt = jwt;
  }

  @Override
  public void applyRequestMetadata(
      MethodDescriptor<?, ?> method,
      Attributes attrs,
      Executor appExecutor,
      MetadataApplier applier) {
    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
        String.format("Bearer %s", jwt));
    applier.apply(metadata);
  }

  @Override
  public void thisUsesUnstableApi() {
    // does nothing
  }
}
