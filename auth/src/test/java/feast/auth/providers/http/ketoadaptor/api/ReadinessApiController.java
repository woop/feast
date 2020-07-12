package feast.auth.providers.http.ketoadaptor.api;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.NativeWebRequest;
import java.util.Optional;
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2020-07-12T11:35:52.408245+08:00[Asia/Singapore]")

@Controller
@RequestMapping("${openapi.feastAuthorizationServer.base-path:}")
public class ReadinessApiController implements ReadinessApi {

    private final NativeWebRequest request;

    @org.springframework.beans.factory.annotation.Autowired
    public ReadinessApiController(NativeWebRequest request) {
        this.request = request;
    }

    @Override
    public Optional<NativeWebRequest> getRequest() {
        return Optional.ofNullable(request);
    }

}
