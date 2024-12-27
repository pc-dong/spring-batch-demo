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
                List<String> shouldDeletedIds = outlet.getShouldDeletedIds();
                shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));
                log.info("Delete outlet item: {}", id);
                if (outlet.getUpdatedAt().isAfter(lastUpdateTime)) {
                    lastUpdateTime = outlet.getUpdatedAt();
                }
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

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, associate);

            List<String> shouldDeletedIds = outlet.getShouldDeletedIds();
            shouldDeletedIds.forEach(shouldDeletedId -> delete(shouldDeletedId, associations));

            log.info("Writing outlet item: {}", id);
            associations.put(associateJson);
            if (outlet.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = outlet.getUpdatedAt();
            }
        }

        push(associations, TABLE_NAME, appName);
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
