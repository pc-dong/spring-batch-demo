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
import java.util.Map;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

@Slf4j
public class SubProductItemWriter extends AbstractOpenSearcherItemWriter<SubProduct> {
    private StepExecution stepExecution;
    private String appName;

    public SubProductItemWriter(OpenSearchProperties openSearchProperties,
                                DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "sub_products";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends SubProduct> chunk) throws Exception {
        JSONArray products = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (SubProduct product : chunk) {
            String product_uuid = product.getProduct_uuid();
            if (product.shouldDeleted()) {
                delete(product_uuid, products);
                log.info("Delete subProduct item: {}", product_uuid);

                continue;
            }

            Map<String, Object> productMap = Maps.newLinkedHashMap();
            productMap.put("sub_product_uuid", product_uuid);
            productMap.put("sub_product_tag_names", product.getProduct_tag_names());
            productMap.put("sub_product_status", product.getProduct_status());
            productMap.put("sub_product_fuzzy_title", product.getProduct_title());
            productMap.put("sub_product_created_at", localDateTimeToLong(product.getProduct_created_at()));
            productMap.put("sub_product_subtype", product.getProduct_subtype());
            productMap.put("sub_product_id", product.getProduct_id());
            productMap.put("sub_product_member_only", product.getProduct_member_only() ? 1:0);
            productMap.put("sub_product_preheat_date", localDateTimeToLong(product.getProduct_preheat_date()));
            productMap.put("sub_product_value", String.valueOf(product.getProduct_value()));
            productMap.put("sub_product_inventory", product.getProduct_inventory());
            productMap.put("sub_product_fuzzy_tag_names", product.getProduct_tag_names());
            productMap.put("sub_product_cuisine_name_cn", product.getProduct_cuisine_name_cn());
            productMap.put("sub_product_cuisine_name_cn_fz", product.getProduct_cuisine_name_cn());
            productMap.put("sub_product_warm_up_type", product.getProduct_warm_up_type());
            productMap.put("sub_product_dining_periods", product.getProductDiningPeriods());
            productMap.put("sub_product_title", product.getProduct_title());
            productMap.put("sub_product_deleted_at", localDateTimeToLong(product.getProduct_deleted_at()));
            productMap.put("sub_product_sale_price", String.valueOf(product.getProduct_sale_price()));
            productMap.put("sub_product_type", product.getProduct_type());
            productMap.put("sub_product_sale_stop_at", localDateTimeToLong(product.getProduct_sale_stop_at()));
            productMap.put("sub_product_cuisine_id", product.getProduct_cuisine_id());
            productMap.put("sub_product_media_type", product.getProduct_media_type());
            productMap.put("sub_product_sale_start_at", localDateTimeToLong(product.getProduct_sale_start_at()));
            productMap.put("sub_product_markdown_price", String.valueOf(product.getProduct_markdown_price()));
            productMap.put("sub_product_invisible", product.getProduct_invisible() );
            productMap.put("sub_product_sold_qty", product.getProduct_sold_qty());
            productMap.put("product_video_cover_img_url", product.getProduct_video_cover_image_url());

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, productMap);


            log.info("Writing subProduct item: {}", product_uuid);
            products.put(associateJson);
            if (product.getUpdatedAt().isAfter(lastUpdateTime)) {
                lastUpdateTime = product.getUpdatedAt();
            }
        }

        push(products, TABLE_NAME, appName);
        stepExecution.getJobExecution().getExecutionContext().put(UPDATED_UPDATE_TIME, lastUpdateTime);
        log.info("Last update time: {}", lastUpdateTime);
    }

    private static Long localDateTimeToLong(LocalDateTime localDateTime) {
        return Optional.ofNullable(localDateTime)
                .map(time -> time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()).orElse(null);
    }

    private static void delete(String sub_product_uuid, JSONArray associations) {
        JSONObject deleteJson = new JSONObject();
        deleteJson.put(DocumentConstants.DOC_KEY_CMD, Command.DELETE.toString());
        deleteJson.put(DocumentConstants.DOC_KEY_FIELDS, Map.of("sub_product_uuid", sub_product_uuid));
        associations.put(deleteJson);
    }
}
