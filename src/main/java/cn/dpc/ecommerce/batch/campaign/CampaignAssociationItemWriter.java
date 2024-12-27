package cn.dpc.ecommerce.batch.campaign;

import cn.dpc.ecommerce.batch.location.OutletAssociation;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;
import static cn.dpc.ecommerce.batch.consts.Constants.NULL_UUID;
import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

@Slf4j
public class CampaignAssociationItemWriter extends AbstractOpenSearcherItemWriter<CampaignAssociation> {
    private StepExecution stepExecution;
    private String appName;

    public CampaignAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                         DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "entity_associations";
    private static final String ASSOCIATIONS_TYPE = "CAMPAIGN_ASSOCIATION";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends CampaignAssociation> chunk) throws Exception {
        JSONArray associations = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (CampaignAssociation campaign : chunk) {
            String id = campaign.getAssociationId();
            if (campaign.shouldDeleted()) {
                delete(id, associations);
                // campaign deleted then all associations should be deleted
                if (campaign.shouldCampaignDeleted()) {
                    List<String> shouldDeletedIds = campaign.getShouldDeletedIdsForDelete();
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));

                    lastUpdateTime = getLastUpdateTime(campaign, lastUpdateTime);
                    continue;
                }

                // just campaign offer deleted then need insert a new association
                String newId = campaign.getAssociationId(campaign.getCampaign_id(), NULL_ID);
                Map<String, Object> associate = Maps.newLinkedHashMap();
                associate.put("id", newId);
                associate.put("association_type", ASSOCIATIONS_TYPE);
                associate.put("campaign_uuid", campaign.getCampaign_uuid());
                associate.put("campaign_offer_id", NULL_ID);

                JSONObject associateJson = new JSONObject();
                associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
                associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);
                log.info("Writing campaign item: {}", newId);
                associations.put(associateJson);

                lastUpdateTime = getLastUpdateTime(campaign, lastUpdateTime);
                continue;
            }

            Map<String, Object> associate = Maps.newLinkedHashMap();
            associate.put("id", id);
            associate.put("association_type", ASSOCIATIONS_TYPE);
            associate.put("campaign_uuid", campaign.getCampaign_uuid());
            associate.put("campaign_offer_id", campaign.getCampaign_offer_uuid());
            log.info("Writing campaign item: {}", id);
            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);

            List<String> shouldDeletedIds = campaign.getShouldDeletedIdsForUpdate();
            shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));

            associations.put(associateJson);
            lastUpdateTime = getLastUpdateTime(campaign, lastUpdateTime);
        }

        push(associations, TABLE_NAME, appName);
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private static LocalDateTime getLastUpdateTime(CampaignAssociation campaign, LocalDateTime lastUpdateTime) {
        if (campaign.getUpdatedAt().isAfter(lastUpdateTime)) {
            lastUpdateTime = campaign.getUpdatedAt();
        }
        return lastUpdateTime;
    }

    private static void delete(String id, JSONArray associations) {
        log.info("Delete campaign item: {}", id);
        JSONObject deleteJson = new JSONObject();
        deleteJson.put(DocumentConstants.DOC_KEY_CMD, Command.DELETE.toString());
        deleteJson.put(DocumentConstants.DOC_KEY_FIELDS, Map.of("id", id));
        associations.put(deleteJson);
    }
}
