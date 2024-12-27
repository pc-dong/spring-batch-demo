package cn.dpc.ecommerce.batch.campaign;

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
import java.util.Map;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

@Slf4j
public class CampaignItemWriter extends AbstractOpenSearcherItemWriter<Campaign> {
    private StepExecution stepExecution;
    private String appName;

    public CampaignItemWriter(OpenSearchProperties openSearchProperties,
                              DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "campaigns";
    private static final String TABLE_KEY = "campaign_uuid";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends Campaign> chunk) throws Exception {
        JSONArray campaignJsonArray = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (Campaign campaign : chunk) {
            String uuid = campaign.getCampaign_uuid();
            if (campaign.shouldDeleted()) {
                delete(uuid, campaignJsonArray);
                log.info("Delete campaign: {}", uuid);
                if (campaign.getUpdatedAt().isAfter(lastUpdateTime)) {
                    lastUpdateTime = campaign.getUpdatedAt();
                }
                continue;
            }

            Map<String, Object> updatedCampaign = Maps.newLinkedHashMap();
            updatedCampaign.put(TABLE_KEY, uuid);
            updatedCampaign.put("campaign_id", campaign.getCampaign_id());
            updatedCampaign.put("campaign_name", campaign.getCampaign_name());
            updatedCampaign.put("campaign_subtitle", campaign.getCampaign_subtitle());
            updatedCampaign.put("campaign_tag_names", campaign.getCampaign_tag_names());
            updatedCampaign.put("campaign_type", campaign.getCampaign_type());
            updatedCampaign.put("campaign_warm_up_type", campaign.getCampaign_warm_up_type());
            updatedCampaign.put("campaign_enabled", campaign.getCampaign_enabled());
            updatedCampaign.put("campaign_start_at", Optional.of(campaign.getCampaign_start_at()).map(localDateTime -> localDateTime
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()).orElse(null));
            updatedCampaign.put("campaign_end_at", Optional.of(campaign.getCampaign_end_at()).map(localDateTime -> localDateTime
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()).orElse(null));
            updatedCampaign.put("campaign_online_at", campaign.getCampaign_online_at());
            updatedCampaign.put("campaign_warm_up_date", campaign.getCampaign_warm_up_date());
            updatedCampaign.put("campaign_warm_up", campaign.getCampaign_warm_up());
            updatedCampaign.put("campaign_description", campaign.getCampaign_description());
            updatedCampaign.put("campaign_image", campaign.getCampaign_image());
            updatedCampaign.put("campaign_tags", campaign.getCampaign_tags());

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, updatedCampaign);

            log.info("Writing campaign item: {}", uuid);
            campaignJsonArray.put(associateJson);
            if (campaign.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = campaign.getUpdatedAt();
            }
        }

        push(campaignJsonArray, TABLE_NAME, appName);
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private static void delete(String id, JSONArray associations) {
        JSONObject deleteJson = new JSONObject();
        deleteJson.put(DocumentConstants.DOC_KEY_CMD, Command.DELETE.toString());
        deleteJson.put(DocumentConstants.DOC_KEY_FIELDS, Map.of(TABLE_KEY, id));
        associations.put(deleteJson);
    }
}
