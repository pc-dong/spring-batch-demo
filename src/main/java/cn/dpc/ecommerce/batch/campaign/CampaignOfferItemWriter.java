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
public class CampaignOfferItemWriter extends AbstractOpenSearcherItemWriter<CampaignOffer> {
    private StepExecution stepExecution;
    private String appName;

    public CampaignOfferItemWriter(OpenSearchProperties openSearchProperties,
                                   DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "campaign_offers";
    private static final String TABLE_KEY = "campaign_offer_uuid";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends CampaignOffer> chunk) throws Exception {
        JSONArray campaignJsonArray = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (CampaignOffer campaignOffer : chunk) {
            String uuid = campaignOffer.getCampaign_offer_uuid();
            if (campaignOffer.shouldDeleted()) {
                delete(uuid, campaignJsonArray);
                log.info("Delete campaign offer: {}", uuid);
                if (campaignOffer.getUpdatedAt().isAfter(lastUpdateTime)) {
                    lastUpdateTime = campaignOffer.getUpdatedAt();
                }
                continue;
            }

            Map<String, Object> updatedCampaign = Maps.newLinkedHashMap();
            updatedCampaign.put(TABLE_KEY, uuid);
            updatedCampaign.put("campaign_offer_id", campaignOffer.getCampaign_offer_id());
            updatedCampaign.put("campaign_offer_name", campaignOffer.getCampaign_offer_name());

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, updatedCampaign);

            log.info("Writing campaign offer item: {}", uuid);
            campaignJsonArray.put(associateJson);
            if (campaignOffer.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = campaignOffer.getUpdatedAt();
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
