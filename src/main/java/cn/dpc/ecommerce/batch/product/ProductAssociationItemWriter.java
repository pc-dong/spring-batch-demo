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
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_UUID;
import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;
import static cn.dpc.ecommerce.batch.product.ProductAssociations.CAMPAIGN_OFFER;
import static cn.dpc.ecommerce.batch.product.ProductAssociations.OUTLET;
import static cn.dpc.ecommerce.batch.product.ProductAssociations.PROPERTY;
import static cn.dpc.ecommerce.batch.product.ProductAssociations.PURCHASE_TIME;
import static cn.dpc.ecommerce.batch.product.ProductAssociations.SUB_PRODUCT;

@Slf4j
public class ProductAssociationItemWriter extends AbstractOpenSearcherItemWriter<ProductAssociations> {
    private StepExecution stepExecution;

    public ProductAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                        DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "entity_associations";
    private static final String ASSOCIATIONS_TYPE = "PRODUCT_ASSOCIATION";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public void write(Chunk<? extends ProductAssociations> chunk) throws Exception {
        JSONArray associations = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (ProductAssociations product : chunk) {
            String id = product.getAssociationId();
            if (product.shouldDeleted()) {
                delete(id, associations);
                List<String> shouldDeletedIds = product.getShouldDeletedIdsForDelete();
                shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));
                continue;
            }

            Map<String, Object> associate = Maps.newLinkedHashMap();
            associate.put("id", id);
            associate.put("association_type", ASSOCIATIONS_TYPE);
            associate.put("product_uuid", product.getSub_product_uuid());
            associate.put("sub_product_uuid", product.getProduct_uuid());
            associate.put("property_uuid", product.getProperty_uuid());
            associate.put("campaign_uuid", product.getCampaign_uuid());
            associate.put("campaign_offer_uuid", product.getProduct_campaign_offer_uuid());
            associate.put("outlet_uuid", product.getOutlet_uuid());
            associate.put("outlet_cuisine_uuid", NULL_UUID);
            associate.put("constraint_purchase_time_uuid", product.getProduct_purchase_time_uuid());
            associate.put("constraint_purchase_start_time", product.getPurchase_start_time()
                    .map(localDateTime -> localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                    .orElse(null));
            associate.put("constraint_purchase_end_time", product.getPurchase_end_time().map(localDateTime -> localDateTime
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()).orElse(null));
            associate.put("product_promotion_rank", product.getProduct_promotion_rank());
            associate.put("property_promotion_rank", product.getProduct_property_rank());
            associate.put("campaign_promotion_rank", product.getProduct_offer_rank());

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);

            List<String> shouldDeletedIds = product.getShouldDeletedIdsForUpdate();
            shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));

            log.info("Writing item: {}", product.getAssociationId());
            associations.put(associateJson);
            if (product.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = product.getUpdatedAt();
            }
        }

        push(associations, TABLE_NAME);
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private static void delete(String id, JSONArray associations) {
        JSONObject deleteJson = new JSONObject();
        deleteJson.put(DocumentConstants.DOC_KEY_CMD, Command.DELETE.toString());
        deleteJson.put(DocumentConstants.DOC_KEY_FIELDS, Map.of("id", id));
        associations.put(deleteJson);
    }
}
