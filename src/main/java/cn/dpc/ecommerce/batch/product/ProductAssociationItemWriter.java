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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;
import static cn.dpc.ecommerce.batch.consts.Constants.NULL_UUID;
import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;
import static cn.dpc.ecommerce.batch.product.ProductAssociations.getAssociationId;

@Slf4j
public class ProductAssociationItemWriter extends AbstractOpenSearcherItemWriter<ProductAssociations> {
    private StepExecution stepExecution;
    private String appName;
    private static final Executor executor = Executors.newCachedThreadPool();

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
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (ProductAssociations product : chunk) {
            String id = product.getAssociationId();
            if (product.shouldDeleted()) {
                delete(id, associations);
                if (product.shouldProductDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForDelete();
                    JSONArray finalAssociations1 = associations;
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, finalAssociations1));
                    lastUpdateTime = getLastUpdateTime(product, lastUpdateTime);
                    associations = checkPush(associations, futures);
                    continue;
                }

                if(product.shouldSubProductDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForSubProductDelete();
                    JSONArray finalAssociations2 = associations;
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, finalAssociations2));
                }

                if(product.shouldProductPurchaseTimeDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForPurchaseTimeDelete();
                    JSONArray finalAssociations3 = associations;
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, finalAssociations3));
                }

                if(product.shouldCampaignOfferDeleted()) {
                    List<String> shouldDeletedIds = product.getShouldDeletedIdsForCampaignDelete();
                    JSONArray finalAssociations4 = associations;
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, finalAssociations4));
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
                associations = checkPush(associations, futures);
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
            JSONArray finalAssociations5 = associations;
            shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, finalAssociations5));

            log.info("Writing productAssociation item: {}", id);
            associations.put(associateJson);

            lastUpdateTime = getLastUpdateTime(product, lastUpdateTime);
            associations = checkPush(associations, futures);
        }
        if (associations.length() != 0) {
            doPush(associations, futures);
        }

//        push(associations, TABLE_NAME, appName);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        for (CompletableFuture<Boolean> future : futures) {
            if (!future.get()) {
                throw new RuntimeException("Error pushing data to OpenSearch");
            }
        }
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private void doPush(JSONArray associations, List<CompletableFuture<Boolean>> futures) {
        futures.add(CompletableFuture.supplyAsync(() -> {
            try {
                push(associations, TABLE_NAME, appName);
                return true;
            } catch (Exception e) {
                log.error("Error pushing data to OpenSearch", e);
                return false;
            }
        }, executor));
    }

    private JSONArray checkPush(JSONArray associations, List<CompletableFuture<Boolean>> futures) {
        if(associations.length() >= 100) {
            doPush(associations, futures);
            associations = new JSONArray();
        }
        return associations;
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
