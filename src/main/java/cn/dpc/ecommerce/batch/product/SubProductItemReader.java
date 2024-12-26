package cn.dpc.ecommerce.batch.product;

import cn.dpc.ecommerce.batch.consts.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class SubProductItemReader implements ItemReader<SubProduct> {
    private final JdbcPagingItemReader<SubProduct> reader;

    public SubProductItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        this.reader = new JdbcPagingItemReaderBuilder<SubProduct>()
                .name("subProductItemReader")
                .dataSource(dataSource)
                .rowMapper(SubProduct.getRowMapper())
                .selectClause(selectClause)
                .fromClause(fromClause)
                .sortKeys(getSortKeys())
                .pageSize(pageSize)
                .fetchSize(fetchSize)
                .build();
    }


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        LocalDateTime lastUpdateTime = Optional.ofNullable(executionContext.get(Constants.LAST_UPDATE_TIME))
                .map(o -> (LocalDateTime) o).orElse(LocalDateTime.now().minusYears(5));
        LocalDateTime endTime = Optional.ofNullable(executionContext.get(Constants.END_TIME, LocalDateTime.class))
                .orElse(LocalDateTime.now());

        this.reader.setParameterValues(Map.of("startTime", lastUpdateTime, "endTime", endTime));
        log.info("Last update time in reader: {}", lastUpdateTime);
    }

    @Override
    public SubProduct read() throws Exception {
        this.reader.afterPropertiesSet();
        return this.reader.read();
    }

    private static String selectClause = """
            pa.*
            """;
    private static String fromClause = """
            (select p.id                    as product_id,
                    p.uuid                  as product_uuid,
                    p.type                  as product_type,
                    p.subtype               as product_subtype,
                    p.status                as product_status,
                    p.title				   as product_title,
                    p.warm_up_type          as product_warm_up_type,
                    p.has_online_flag       as product_has_online_flag,
                    p.dining_periods        as product_dining_periods,
                    p.sale_price            as product_sale_price,
                    p.markdown_price        as product_markdown_price,
                    p.invisible             as product_invisible,
                    p.preheat_date          as product_preheat_date,
                    p.sale_start_at         as product_sale_start_at,
                    p.sale_stop_at          as product_sale_stop_at,
                    p.cuisine_id            as product_cuisine_id,
                    p.media_type            as product_media_type,
                    p.cover_image_url       as product_cover_image_url,
                    p.video_cover_image_url as product_video_cover_image_url,
                    (select GROUP_CONCAT(pi.image_url)
                     from products_images pi
                     where p.id = pi.product_id
                       and pi.type = 'BANNER'
                       AND pi.deleted_at is null
                     limit 5)
                                            as product_banner_cover_image_url,
                    p.value                 as product_value,
                    p.sold_qty              as product_sold_qty,
                    p.inventory             as product_inventory,
                    p.member_only           as product_member_only,
                    p.current_version_uuid  as product_current_version_uuid,
                    p.created_at            as product_created_at,
                    p.deleted_at            as product_deleted_at,
                    p.updated_at            as product_updated_at,
                    (select GROUP_CONCAT(tags.name)
                     from products_tags pt
                              inner join tags on pt.tag_uuid = tags.uuid
                     where pt.product_id = p.id
                       and pt.deleted_at is null
                       and tags.deleted_at is null)
                                            as product_tag_names
             from products p
                      left join product_cuisines pc on p.cuisine_id = pc.id and pc.deleted_at is null
             where (p.status != 'DRAFT' OR p.has_online_flag = 1) and p.bundle_product_id is not null
                and (p.updated_at > :startTime and p.updated_at < :endTime
                or p.deleted_at > :startTime and p.deleted_at < :endTime
                or exists (select 1
                           from product_cuisines pc
                           where pc.id = p.cuisine_id and pc.updated_at > :startTime and pc.updated_at < :endTime
                              or pc.deleted_at > :startTime and pc.deleted_at < :endTime)
                or exists (select 1
                           from products_tags tag
                                    inner join tags
                           where tag.product_id = p.id and tag.tag_uuid = tags.uuid
                               and tag.updated_at > :startTime and tag.updated_at < :endTime
                              or tag.deleted_at > :startTime and tag.deleted_at < :endTime
                              or tags.updated_at > :startTime and tags.updated_at < :endTime)
              )) pa
            """;

    private Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("product_id", Order.ASCENDING);
        return sortKeys;
    }
}
