package cn.dpc.ecommerce.batch.opensearch;

import com.aliyun.opensearch.DocumentClient;
import com.aliyun.opensearch.OpenSearchClient;
import com.aliyun.opensearch.sdk.generated.OpenSearch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenSearchConfig {
    @Autowired
    private OpenSearchProperties openSearchProperties;

    @Bean
    public DocumentClient documentClient(OpenSearchClient openSearchClient) {
        return new DocumentClient(openSearchClient);
    }

    @Bean
    public OpenSearch openSearch() {
        return new OpenSearch(openSearchProperties.getAccessKey(), openSearchProperties.getSecretKey(), openSearchProperties.getHost());
    }

    @Bean
    public OpenSearchClient openSearchClient(OpenSearch openSearch) {
        return new OpenSearchClient(openSearch);
    }
}
