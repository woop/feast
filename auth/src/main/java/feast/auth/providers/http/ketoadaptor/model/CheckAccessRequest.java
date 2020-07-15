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
 * CheckAccessRequest
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2020-07-16T00:07:27.755245+08:00[Asia/Singapore]")

public class CheckAccessRequest   {
  @JsonProperty("action")
  private String action;

  @JsonProperty("context")
  private Object context;

  @JsonProperty("resource")
  private String resource;

  @JsonProperty("subject")
  private String subject;

  public CheckAccessRequest action(String action) {
    this.action = action;
    return this;
  }

  /**
   * Action is the action that is being taken on the requested resource.
   * @return action
  */
  @ApiModelProperty(value = "Action is the action that is being taken on the requested resource.")


  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public CheckAccessRequest context(Object context) {
    this.context = context;
    return this;
  }

  /**
   * Context is the request's environmental context.
   * @return context
  */
  @ApiModelProperty(value = "Context is the request's environmental context.")

  @Valid

  public Object getContext() {
    return context;
  }

  public void setContext(Object context) {
    this.context = context;
  }

  public CheckAccessRequest resource(String resource) {
    this.resource = resource;
    return this;
  }

  /**
   * Resource is the resource that access is requested to.
   * @return resource
  */
  @ApiModelProperty(value = "Resource is the resource that access is requested to.")


  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public CheckAccessRequest subject(String subject) {
    this.subject = subject;
    return this;
  }

  /**
   * Subject is the subject that is requesting access, typically the user.
   * @return subject
  */
  @ApiModelProperty(value = "Subject is the subject that is requesting access, typically the user.")


  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CheckAccessRequest checkAccessRequest = (CheckAccessRequest) o;
    return Objects.equals(this.action, checkAccessRequest.action) &&
        Objects.equals(this.context, checkAccessRequest.context) &&
        Objects.equals(this.resource, checkAccessRequest.resource) &&
        Objects.equals(this.subject, checkAccessRequest.subject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(action, context, resource, subject);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CheckAccessRequest {\n");
    
    sb.append("    action: ").append(toIndentedString(action)).append("\n");
    sb.append("    context: ").append(toIndentedString(context)).append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
    sb.append("    subject: ").append(toIndentedString(subject)).append("\n");
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

