package cn.dpc.ecommerce.batch.product;

import cn.dpc.ecommerce.batch.opensearch.AbstractOpenSearcherItemWriter;
import cn.dpc.ecommerce.batch.opensearch.OpenSearchProperties;
import com.aliyun.opensearch.DocumentClient;
import com.aliyun.opensearch.sdk.dependencies.com.google.common.collect.Maps;
import com.aliyun.opensearch.sdk.dependencies.org.json.JSONArray;
import com.aliyun.opensearch.sdk.dependencies.org.json.JSONObject;
import com.aliyun.opensearch.sdk.generated.document.Command;
import com.aliyun.opensearch.sdk.generated.document.DocumentConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;

import java.time.LocalDateTime;
import java.util.Map;

import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

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
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
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
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }
}
