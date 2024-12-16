package cn.dpc.ecommerce.batch.opensearch;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "opensearch")
@Component
@Data
public class OpenSearchProperties {
    private String appName;
    private String host;
    private String accessKey;
    private String secretKey;
}
