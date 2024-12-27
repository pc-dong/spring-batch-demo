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

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;
import static cn.dpc.ecommerce.batch.consts.Constants.NULL_UUID;
import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;
import static cn.dpc.ecommerce.batch.product.ProductAssociations.getAssociationId;

@Slf4j
public class ProductAssociationItemWriter extends AbstractOpenSearcherItemWriter<ProductAssociations> {
    private StepExecution stepExecution;
    private String appName;

    public ProductAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                        DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "entity_associations";
    private static final String ASSOCIATIONS_TYPE = "PRODUCT_ASSOCIATION";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends ProductAssociations> chunk) throws Exception {
        JSONArray associations = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (ProductAssociations product : chunk) {
            String id = product.getAssociationId();
            if (product.shouldDeleted()) {
                delete(id, associations);
                if (product.shouldProductDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForDelete();
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));
                    lastUpdateTime = getLastUpdateTime(product, lastUpdateTime);
                    continue;
                }

                if(product.shouldSubProductDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForSubProductDelete();
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));
                }

                if(product.shouldProductPurchaseTimeDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForPurchaseTimeDelete();
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));
                }

                if(product.shouldCampaignOfferDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForCampaignDelete();
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));
                }

                // partial deleted then need insert a new association
                Map<String, Object> associate = Maps.newLinkedHashMap();
                String newId = getAssociationId(product.getProduct_id(),
                        product.shouldSubProductDeleted() ? NULL_ID : product.getSub_product_id(),
                        product.getOutletOrPropertyId(),
                        product.shouldCampaignOfferDeleted() ? NULL_ID : product.getProduct_campaign_offer_id(),
                        product.shouldProductPurchaseTimeDeleted() ? NULL_ID : product.getProduct_purchase_time_id());
                associate.put("id", newId);
                associate.put("association_type", ASSOCIATIONS_TYPE);
                associate.put("product_uuid", product.getProduct_uuid());
                associate.put("sub_product_uuid", product.shouldSubProductDeleted() ? NULL_UUID :  product.getSub_product_uuid());
                associate.put("property_uuid", product.getProperty_uuid());
                associate.put("campaign_uuid", product.shouldCampaignDeleted() ? NULL_UUID : product.getCampaign_uuid());
                associate.put("campaign_offer_uuid",product.shouldCampaignDeleted() ? NULL_UUID : product.getProduct_campaign_offer_uuid());
                associate.put("outlet_uuid", product.getOutlet_uuid());
                associate.put("outlet_cuisine_uuid", NULL_UUID);
                associate.put("constraint_purchase_time_uuid", product.shouldProductPurchaseTimeDeleted() ? NULL_UUID : product.getProduct_purchase_time_uuid());
                associate.put("constraint_purchase_start_time", product.shouldProductPurchaseTimeDeleted() ? null : product.getPurchase_start_time()
                        .map(localDateTime -> localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                        .orElse(null));
                associate.put("constraint_purchase_end_time", product.shouldProductPurchaseTimeDeleted() ? null : product.getPurchase_end_time().map(localDateTime -> localDateTime
                        .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()).orElse(null));
                associate.put("product_promotion_rank", product.getProduct_promotion_rank());
                associate.put("property_promotion_rank", product.getProduct_property_rank());
                associate.put("campaign_promotion_rank", product.getProduct_offer_rank());

                JSONObject associateJson = new JSONObject();
                associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
                associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);
                log.info("Writing productAssociation item: {}", newId);
                associations.put(associateJson);

                lastUpdateTime = getLastUpdateTime(product, lastUpdateTime);
                continue;
            }

            Map<String, Object> associate = Maps.newLinkedHashMap();
            associate.put("id", id);
            associate.put("association_type", ASSOCIATIONS_TYPE);
            associate.put("product_uuid", product.getProduct_uuid());
            associate.put("sub_product_uuid", product.getSub_product_uuid());
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

            log.info("Writing productAssociation item: {}", id);
            associations.put(associateJson);

            lastUpdateTime = getLastUpdateTime(product, lastUpdateTime);
        }

        push(associations, TABLE_NAME, appName);
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private LocalDateTime getLastUpdateTime(ProductAssociations product, LocalDateTime lastUpdateTime) {
        if (product.getUpdatedAt().isAfter(lastUpdateTime)) {
            lastUpdateTime = product.getUpdatedAt();
        }
        return lastUpdateTime;
    }

    private static void delete(String id, JSONArray associations) {
        log.info("Delete productAssociation item: {}", id);
        JSONObject deleteJson = new JSONObject();
        deleteJson.put(DocumentConstants.DOC_KEY_CMD, Command.DELETE.toString());
        deleteJson.put(DocumentConstants.DOC_KEY_FIELDS, Map.of("id", id));
        associations.put(deleteJson);
    }
}
