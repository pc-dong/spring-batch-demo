package cn.dpc.ecommerce.batch.location;

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
public class OutletItemWriter extends AbstractOpenSearcherItemWriter<Outlet> {
    private StepExecution stepExecution;
    private String appName;

    public OutletItemWriter(OpenSearchProperties openSearchProperties,
                            DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "outlets";
    private static final String TABLE_KEY = "outlet_uuid";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends Outlet> chunk) throws Exception {
        JSONArray outletJsonArray = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (Outlet outlet : chunk) {
            String uuid = outlet.getOutlet_uuid();
            if (outlet.shouldDeleted()) {
                delete(uuid, outletJsonArray);
                log.info("Delete outlet: {}", uuid);
                if (outlet.getUpdatedAt().isAfter(lastUpdateTime)) {
                    lastUpdateTime = outlet.getUpdatedAt();
                }
                continue;
            }

            Map<String, Object> updatedOutlet = Maps.newLinkedHashMap();
            updatedOutlet.put(TABLE_KEY, uuid);
            updatedOutlet.put("outlet_id", outlet.getOutlet_id());
            updatedOutlet.put("outlet_name_chinese", outlet.getOutlet_name_chinese());
            updatedOutlet.put("outlet_name_english", outlet.getOutlet_name_english());
            updatedOutlet.put("outlet_type", outlet.getOutlet_type());
            updatedOutlet.put("outlet_status", outlet.getOutlet_status());
            updatedOutlet.put("outlet_service", outlet.getOutlet_service());
            updatedOutlet.put("outlet_rank", outlet.getOutlet_rank());
            updatedOutlet.put("outlet_updated_at", Optional.of(outlet.getOutlet_updated_at()).map(localDateTime -> localDateTime
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()).orElse(null));
            updatedOutlet.put("outlet_operating_hours", outlet.getOutlet_operating_hours());
            updatedOutlet.put("outlet_arrangement_chinese", outlet.getOutlet_arrangement_chinese());
            updatedOutlet.put("outlet_arrangement_english", outlet.getOutlet_arrangement_english());
            updatedOutlet.put("outlet_details_chinese", outlet.getOutlet_details_chinese());
            updatedOutlet.put("outlet_details_english", outlet.getOutlet_details_english());
            updatedOutlet.put("outlet_media_type", outlet.getOutlet_media_type());
            updatedOutlet.put("outlet_video_cover_image_url", outlet.getOutlet_video_cover_image_url());
            updatedOutlet.put("outlet_cover_image_url", outlet.getOutlet_cover_image_url());
            updatedOutlet.put("outlet_banner_image_urls", outlet.getOutlet_banner_image_urls());
            updatedOutlet.put("outlet_address", outlet.getOutlet_address());
            updatedOutlet.put("outlet_short_desc_chinese", outlet.getOutlet_short_desc_chinese());
            updatedOutlet.put("outlet_mobile_phone", outlet.getOutlet_mobile_phone());
            updatedOutlet.put("outlet_mobile_country_code", outlet.getOutlet_mobile_country_code());
            updatedOutlet.put("outlet_landline", outlet.getOutlet_landline());
            updatedOutlet.put("outlet_region_code", outlet.getOutlet_region_code());
            updatedOutlet.put("outlet_landline_country_code", outlet.getOutlet_landline_country_code());
            updatedOutlet.put("outlet_landline_extension", outlet.getOutlet_landline_extension());
            updatedOutlet.put("outlet_online_booking_flag", outlet.getOutlet_online_booking_flag());

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, updatedOutlet);

            log.info("Writing outlet item: {}", uuid);
            outletJsonArray.put(associateJson);
            if (outlet.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = outlet.getUpdatedAt();
            }
        }

        push(outletJsonArray, TABLE_NAME, appName);
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
