package cn.dpc.ecommerce.batch.opensearch;

import com.aliyun.opensearch.DocumentClient;
import org.springframework.batch.item.ItemWriter;

public abstract class AbstractOpenSearcherItemWriter<T> implements ItemWriter<T> {

    protected final OpenSearchProperties openSearchProperties;
    protected final DocumentClient documentClient;

    public AbstractOpenSearcherItemWriter(OpenSearchProperties openSearchProperties, DocumentClient documentClient) {
        this.openSearchProperties = openSearchProperties;
        this.documentClient = documentClient;
    }


}
