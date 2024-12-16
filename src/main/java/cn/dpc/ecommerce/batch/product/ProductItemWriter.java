package cn.dpc.ecommerce.batch.product;

import cn.dpc.ecommerce.batch.opensearch.AbstractOpenSearcherItemWriter;
import cn.dpc.ecommerce.batch.opensearch.OpenSearchProperties;
import com.aliyun.opensearch.DocumentClient;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Maps;
import com.aliyun.opensearch.sdk.dependencies.org.json.JSONArray;
import com.aliyun.opensearch.sdk.dependencies.org.json.JSONObject;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchClientException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchException;
import com.aliyun.opensearch.sdk.generated.commons.OpenSearchResult;
import com.aliyun.opensearch.sdk.generated.document.Command;
import com.aliyun.opensearch.sdk.generated.document.DocumentConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
public class ProductItemWriter extends AbstractOpenSearcherItemWriter<Product> {
    private StepExecution stepExecution;

    public ProductItemWriter(OpenSearchProperties openSearchProperties,
                             DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String PRODUCT_TABLE_NAME = "products";
    private static final String ENTITY_ASSOCIATIONS_TABLE_NAME = "entity_associations";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public void write(Chunk<? extends Product> chunk) throws Exception {
        JSONArray docsJsonArr = new JSONArray();
        JSONArray associations = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get("lastUpdateTime");
        for (Product product : chunk) {
            Map<String, Object> associate = Maps.newLinkedHashMap();
            associate.put("association_type", "PRODUCT_GROUP");
            associate.put("product_id", product.getUuid());
            associate.put("group_product_rank", product.getGroupProductRank());
            associate.put("group_id", product.getGroupId());
            associate.put("id", product.getUuid());
            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);
            System.out.println("Writing item: " + product.getTitle() + " " + product.getUuid());
            associations.put(associateJson);

            Map<String, Object> doc1 = Maps.newLinkedHashMap();
            doc1.put("product_id", product.getUuid());
            doc1.put("product_title", product.getTitle());
            doc1.put("product_status", product.getStatus());
            JSONObject json = new JSONObject();
            json.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            json.put(DocumentConstants.DOC_KEY_FIELDS, doc1);
            System.out.println("Writing item: " + product.getTitle() + " " + product.getUuid());
            docsJsonArr.put(json);
            if (product.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = product.getUpdatedAt();
            }
        }

        push(associations, ENTITY_ASSOCIATIONS_TABLE_NAME);
        push(docsJsonArr, PRODUCT_TABLE_NAME);
        stepExecution.getJobExecution().getExecutionContext().put("lastUpdateTime", lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private void push(JSONArray docs, String tableName) throws OpenSearchClientException, OpenSearchException {
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
