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
import java.util.Map;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

@Slf4j
public class PropertyItemWriter extends AbstractOpenSearcherItemWriter<Property> {
    private StepExecution stepExecution;
    private String appName;

    public PropertyItemWriter(OpenSearchProperties openSearchProperties,
                              DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "properties";
    private static final String TABLE_KEY = "property_uuid";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends Property> chunk) throws Exception {
        JSONArray propertyJsonArray = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (Property property : chunk) {
            String uuid = property.getProperty_uuid();
            if (property.shouldDeleted()) {
                delete(uuid, propertyJsonArray);
                log.info("Delete property: {}", uuid);
                if (property.getUpdatedAt().isAfter(lastUpdateTime)) {
                    lastUpdateTime = property.getUpdatedAt();
                }
                continue;
            }

            Map<String, Object> updatedProperty = Maps.newLinkedHashMap();
            updatedProperty.put(TABLE_KEY, uuid);
            updatedProperty.put("property_id", property.getProperty_id());
            updatedProperty.put("pr_name_chinese", property.getPr_name_chinese());
            updatedProperty.put("pr_status", property.getPr_status());
            updatedProperty.put("pr_brand_id", property.getPr_brand_id());
            updatedProperty.put("pr_full_brand_code", property.getFullBrandCode());
            updatedProperty.put("pr_sub_brand", property.getPr_sub_brand());
            updatedProperty.put("pr_brand_code", property.getPr_brand_code());
            updatedProperty.put("pr_brand_name_chinese", property.getPr_brand_name_chinese());
            updatedProperty.put("pr_brand_category_name_english", property.getPr_brand_category_name_english());
            updatedProperty.put("pr_brand_category_name_chinese", property.getPr_brand_category_name_chinese());
            updatedProperty.put("pr_brand_category_sorting", property.getPr_brand_category_sorting());
            updatedProperty.put("pr_brand_sorting", property.getPr_brand_sorting());
            updatedProperty.put("pr_brand_link_color", property.getPr_brand_link_color());
            updatedProperty.put("pr_brand_primary_color", property.getPr_brand_primary_color());
            updatedProperty.put("pr_brand_logo_url", property.getPr_brand_logo_url());
            updatedProperty.put("pr_brand_name_english", property.getPr_brand_name_english());
            updatedProperty.put("pr_distance", property.getPr_distance());
            updatedProperty.put("pr_longitude", property.getPr_longitude());
            updatedProperty.put("pr_latitude", property.getPr_latitude());
            updatedProperty.put("pr_city_code", property.getPr_city_code());
            updatedProperty.put("pr_city_title_chinese", property.getPr_city_title_chinese());
            updatedProperty.put("pr_city_title_english", property.getPr_city_title_english());
            updatedProperty.put("pr_city_title_pinyin", property.getPr_city_title_pinyin());
            updatedProperty.put("pr_marsha_code", property.getPr_marsha_code());
            updatedProperty.put("pr_address_chinese", property.getPr_address_chinese());
            updatedProperty.put("pr_address_english", property.getPr_address_english());
            updatedProperty.put("pr_name_english", property.getPr_name_english());
            updatedProperty.put("pr_province_name", property.getPr_province_name());
            updatedProperty.put("pr_city_name", property.getPr_city_name());
            updatedProperty.put("pr_district_name", property.getPr_district_name());
            updatedProperty.put("pr_details_chinese", property.getPr_details_chinese());
            updatedProperty.put("pr_details_english", property.getPr_details_english());
            updatedProperty.put("pr_header_image_url", property.getPr_header_image_url());
            updatedProperty.put("pr_is_reservation_enabled", property.getPr_is_reservation_enabled());
            updatedProperty.put("pr_is_cp_enabled", property.getPr_is_cp_enabled());

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, updatedProperty);

            log.info("Writing property item: {}", uuid);
            propertyJsonArray.put(associateJson);
            if (property.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = property.getUpdatedAt();
            }
        }

        push(propertyJsonArray, TABLE_NAME, appName);
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
