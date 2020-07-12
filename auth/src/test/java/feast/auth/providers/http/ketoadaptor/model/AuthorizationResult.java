package feast.auth.providers.http.ketoadaptor.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.openapitools.jackson.nullable.JsonNullable;
import javax.validation.Valid;
import javax.validation.constraints.*;

/**
 * AuthorizationResult
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2020-07-12T11:35:52.408245+08:00[Asia/Singapore]")

public class AuthorizationResult   {
  @JsonProperty("allowed")
  private Boolean allowed;

  public AuthorizationResult allowed(Boolean allowed) {
    this.allowed = allowed;
    return this;
  }

  /**
   * Allowed is true if the request should be allowed and false otherwise.
   * @return allowed
  */
  @ApiModelProperty(required = true, value = "Allowed is true if the request should be allowed and false otherwise.")
  @NotNull


  public Boolean getAllowed() {
    return allowed;
  }

  public void setAllowed(Boolean allowed) {
    this.allowed = allowed;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthorizationResult authorizationResult = (AuthorizationResult) o;
    return Objects.equals(this.allowed, authorizationResult.allowed);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allowed);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuthorizationResult {\n");
    
    sb.append("    allowed: ").append(toIndentedString(allowed)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

