package feast.auth.providers.http.ketoadaptor.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;
import org.openapitools.jackson.nullable.JsonNullable;
import javax.validation.Valid;
import javax.validation.constraints.*;

/**
 * InlineResponse500
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2020-07-12T11:35:52.408245+08:00[Asia/Singapore]")

public class InlineResponse500   {
  @JsonProperty("code")
  private Long code;

  @JsonProperty("details")
  @Valid
  private List<Object> details = null;

  @JsonProperty("message")
  private String message;

  @JsonProperty("reason")
  private String reason;

  @JsonProperty("request")
  private String request;

  @JsonProperty("status")
  private String status;

  public InlineResponse500 code(Long code) {
    this.code = code;
    return this;
  }

  /**
   * Get code
   * @return code
  */
  @ApiModelProperty(value = "")


  public Long getCode() {
    return code;
  }

  public void setCode(Long code) {
    this.code = code;
  }

  public InlineResponse500 details(List<Object> details) {
    this.details = details;
    return this;
  }

  public InlineResponse500 addDetailsItem(Object detailsItem) {
    if (this.details == null) {
      this.details = new ArrayList<>();
    }
    this.details.add(detailsItem);
    return this;
  }

  /**
   * Get details
   * @return details
  */
  @ApiModelProperty(value = "")


  public List<Object> getDetails() {
    return details;
  }

  public void setDetails(List<Object> details) {
    this.details = details;
  }

  public InlineResponse500 message(String message) {
    this.message = message;
    return this;
  }

  /**
   * Get message
   * @return message
  */
  @ApiModelProperty(value = "")


  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public InlineResponse500 reason(String reason) {
    this.reason = reason;
    return this;
  }

  /**
   * Get reason
   * @return reason
  */
  @ApiModelProperty(value = "")


  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public InlineResponse500 request(String request) {
    this.request = request;
    return this;
  }

  /**
   * Get request
   * @return request
  */
  @ApiModelProperty(value = "")


  public String getRequest() {
    return request;
  }

  public void setRequest(String request) {
    this.request = request;
  }

  public InlineResponse500 status(String status) {
    this.status = status;
    return this;
  }

  /**
   * Get status
   * @return status
  */
  @ApiModelProperty(value = "")


  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InlineResponse500 inlineResponse500 = (InlineResponse500) o;
    return Objects.equals(this.code, inlineResponse500.code) &&
        Objects.equals(this.details, inlineResponse500.details) &&
        Objects.equals(this.message, inlineResponse500.message) &&
        Objects.equals(this.reason, inlineResponse500.reason) &&
        Objects.equals(this.request, inlineResponse500.request) &&
        Objects.equals(this.status, inlineResponse500.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(code, details, message, reason, request, status);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class InlineResponse500 {\n");
    
    sb.append("    code: ").append(toIndentedString(code)).append("\n");
    sb.append("    details: ").append(toIndentedString(details)).append("\n");
    sb.append("    message: ").append(toIndentedString(message)).append("\n");
    sb.append("    reason: ").append(toIndentedString(reason)).append("\n");
    sb.append("    request: ").append(toIndentedString(request)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
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

