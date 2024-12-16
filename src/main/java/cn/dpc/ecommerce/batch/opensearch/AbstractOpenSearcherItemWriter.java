package cn.dpc.ecommerce.batch.opensearch;

import com.aliyun.opensearch.DocumentClient;
import com.aliyun.opensearch.sdk.dependencies.org.json.JSONArray;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;

@Slf4j
public abstract class AbstractOpenSearcherItemWriter<T> implements ItemWriter<T> {

    protected final OpenSearchProperties openSearchProperties;
    protected final DocumentClient documentClient;

    public AbstractOpenSearcherItemWriter(OpenSearchProperties openSearchProperties, DocumentClient documentClient) {
        this.openSearchProperties = openSearchProperties;
        this.documentClient = documentClient;
    }

    protected void push(JSONArray docs, String tableName) throws OpenSearchClientException, OpenSearchException {
        if (null == docs || docs.length() == 0) {
            log.info("No data to push");
            return;
        }
        try {
            //执行推送操作
            OpenSearchResult osr = documentClient.push(docs.toString(), openSearchProperties.getAppName(), tableName);

            //判断数据是否推送成功，主要通过判断2处，第一处判断用户方推送是否成功，第二处是应用控制台中有无报错日志
            //用户方推送成功后，也有可能在应用端执行失败，此错误会直接在应用控制台错误日志中生成，比如字段内容转换失败
            if (osr.getResult().equalsIgnoreCase("true")) {
                log.info("用户方推送无报错！\n以下为getTraceInfo推送请求Id:{}", osr.getTraceInfo().getRequestId());
            } else {
                log.info("用户方推送报错！{}", osr.getTraceInfo());
                throw new RuntimeException("用户方推送报错！" + osr.getTraceInfo());
            }
        } catch (Exception e) {
            log.error("OpenSearchException:" , e);
            throw e;
        }
    }

}
