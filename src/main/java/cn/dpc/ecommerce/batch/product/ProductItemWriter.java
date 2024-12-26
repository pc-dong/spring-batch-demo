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
public class ProductItemWriter extends AbstractOpenSearcherItemWriter<Product> {
    private StepExecution stepExecution;
    private String appName;

    public ProductItemWriter(OpenSearchProperties openSearchProperties,
                             DocumentClient documentClient) {
        super(openSearchProperties, documentClient);
    }

    private static final String TABLE_NAME = "products";

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        this.appName = Optional.ofNullable(stepExecution.getJobParameters().getString("appName")).orElse(openSearchProperties.getAppName());
    }

    @Override
    public void write(Chunk<? extends Product> chunk) throws Exception {
        JSONArray products = new JSONArray();
        LocalDateTime lastUpdateTime = (LocalDateTime) stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME);
        for (Product product : chunk) {
            String product_uuid = product.getProduct_uuid();
            if (product.shouldDeleted()) {
                delete(product_uuid, products);
                log.info("Delete product item: {}", product_uuid);

                continue;
            }

            Map<String, Object> productMap = Maps.newLinkedHashMap();
            productMap.put("product_uuid", product_uuid);
            productMap.put("product_tag_names", product.getProduct_tag_names());
            productMap.put("group_status", product.getProduct_group_status());
            productMap.put("distinct_key_param", product.getDistinct_key_param());
            productMap.put("product_status", product.getProduct_status());
            productMap.put("product_fuzzy_title", product.getProduct_title());
            productMap.put("product_created_at", localDateTimeToLong(product.getProduct_created_at()));
            productMap.put("product_subtype", product.getProduct_subtype());
            productMap.put("product_tags", product.getProduct_tags());
            productMap.put("product_current_version_uuid", product.getProduct_current_version_uuid());
            productMap.put("product_id", product.getProduct_id());
            productMap.put("product_member_only", product.getProduct_member_only() ? 1:0);
            productMap.put("product_preheat_date", localDateTimeToLong(product.getProduct_preheat_date()));
            productMap.put("group_rank", product.getProduct_group_rank());
            productMap.put("product_cover_img_url", product.getProduct_cover_image_url());
            productMap.put("product_value", String.valueOf(product.getProduct_value()));
            productMap.put("product_inventory", product.getProduct_inventory());
            productMap.put("product_banner_cover_img_url", product.getProduct_banner_cover_image_url());
            productMap.put("product_fuzzy_tag_names", product.getProduct_tag_names());
            productMap.put("product_cuisine_name_cn", product.getProduct_cuisine_name_cn());
            productMap.put("product_fuzzy_cuisine_name_cn", product.getProduct_cuisine_name_cn());
            productMap.put("product_warm_up_type", product.getProduct_warm_up_type());
            productMap.put("product_dining_periods", product.getProductDiningPeriods());
            productMap.put("product_title", product.getProduct_title());
            productMap.put("product_deleted_at", localDateTimeToLong(product.getProduct_deleted_at()));
            productMap.put("product_sale_price", String.valueOf(product.getProduct_sale_price()));
            productMap.put("product_type", product.getProduct_type());
            productMap.put("product_sale_stop_at", localDateTimeToLong(product.getProduct_sale_stop_at()));
            productMap.put("product_cuisine_id", product.getProduct_cuisine_id());
            productMap.put("group_id", product.getProduct_group_id());
            productMap.put("product_media_type", product.getProduct_media_type());
            productMap.put("product_sale_start_at", localDateTimeToLong(product.getProduct_sale_start_at()));
            productMap.put("product_markdown_price", String.valueOf(product.getProduct_markdown_price()));
            productMap.put("product_invisible", product.getProduct_invisible());
            productMap.put("product_sold_qty", product.getProduct_sold_qty());
            productMap.put("product_video_cover_img_url", product.getProduct_video_cover_image_url());

            JSONObject associateJson = new JSONObject();
            associateJson.put(DocumentConstants.DOC_KEY_CMD, Command.ADD.toString());
            associateJson.put(DocumentConstants.DOC_KEY_FIELDS, productMap);


            log.info("Writing product item: {}", product_uuid);
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

    private static void delete(String product_uuid, JSONArray associations) {
        JSONObject deleteJson = new JSONObject();
        deleteJson.put(DocumentConstants.DOC_KEY_CMD, Command.DELETE.toString());
        deleteJson.put(DocumentConstants.DOC_KEY_FIELDS, Map.of("product_uuid", product_uuid));
        associations.put(deleteJson);
    }
}
