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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;
import static cn.dpc.ecommerce.batch.consts.Constants.NULL_UUID;
import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

@Slf4j
public class OutletAssociationItemWriter extends AbstractOpenSearcherItemWriter<OutletAssociation> {
    private StepExecution stepExecution;
    private String appName;

    public OutletAssociationItemWriter(OpenSearchProperties openSearchProperties,
                                       DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "entity_associations";
    private static final String ASSOCIATIONS_TYPE = "OUTLET_ASSOCIATION";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends OutletAssociation> chunk) throws Exception {
        JSONArray associations = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (OutletAssociation outlet : chunk) {
            String id = outlet.getAssociationId();
            if (outlet.shouldDeleted()) {
                delete(id, associations);
                // outlet deleted then all associations should be deleted
                if (outlet.shouldOutletDeleted()) {
                    List<String> shouldDeletedIds = outlet.getShouldDeletedIdsForDelete();
                    shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));

                    lastUpdateTime = getLastUpdateTime(outlet, lastUpdateTime);
                    continue;
                }

                // just cuisine deleted then need insert a new association
                String newId = outlet.getAssociationId(outlet.getOutlet_id(), NULL_ID);
                Map<String, Object> associate = Maps.newLinkedHashMap();
                associate.put("id", newId);
                associate.put("association_type", ASSOCIATIONS_TYPE);
                associate.put("property_uuid", outlet.getProperty_uuid());
                associate.put("outlet_uuid", outlet.getOutlet_uuid());
                associate.put("outlet_cuisine_uuid", NULL_UUID);
                associate.put("cuisine_name_chinese_search", null);
                associate.put("cuisine_name_chinese_filter", null);

                JSONObject associateJson = new JSONObject();
                associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
                associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);
                log.info("Writing outlet item: {}", newId);
                associations.put(associateJson);
                lastUpdateTime = getLastUpdateTime(outlet, lastUpdateTime);
                continue;
            }

            Map<String, Object> associate = Maps.newLinkedHashMap();
            associate.put("id", id);
            associate.put("association_type", ASSOCIATIONS_TYPE);
            associate.put("property_uuid", outlet.getProperty_uuid());
            associate.put("outlet_uuid", outlet.getOutlet_uuid());
            associate.put("outlet_cuisine_uuid", outlet.getCuisine_uuid());
            associate.put("cuisine_name_chinese_search", outlet.getCuisine_name_chinese());
            associate.put("cuisine_name_chinese_filter", outlet.getCuisine_name_chinese());
            log.info("Writing outlet item: {}", id);
            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);

            List<String> shouldDeletedIds = outlet.getShouldDeletedIdsForUpdate();
            shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));

            associations.put(associateJson);
            lastUpdateTime = getLastUpdateTime(outlet, lastUpdateTime);
        }

        push(associations, TABLE_NAME, appName);
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private static LocalDateTime getLastUpdateTime(OutletAssociation outlet, LocalDateTime lastUpdateTime) {
        if (outlet.getUpdatedAt().isAfter(lastUpdateTime)) {
            lastUpdateTime = outlet.getUpdatedAt();
        }
        return lastUpdateTime;
    }

    private static void delete(String id, JSONArray associations) {
        log.info("Delete outlet item: {}", id);
        JSONObject deleteJson = new JSONObject();
        deleteJson.put(DocumentConstants.DOC_KEY_CMD, Command.DELETE.toString());
        deleteJson.put(DocumentConstants.DOC_KEY_FIELDS, Map.of("id", id));
        associations.put(deleteJson);
    }
}
