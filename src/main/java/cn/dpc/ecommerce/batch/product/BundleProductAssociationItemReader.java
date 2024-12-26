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

import static cn.dpc.ecommerce.batch.product.ProductAssociations.getProductAssociationsRowMapper;

@Slf4j
public class BundleProductAssociationItemReader implements ItemReader<ProductAssociations> {
    private final JdbcPagingItemReader<ProductAssociations> reader;

    public BundleProductAssociationItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        this.reader = new JdbcPagingItemReaderBuilder<ProductAssociations>()
                .name("productAssociationItemReader")
                .dataSource(dataSource)
                .rowMapper(getProductAssociationsRowMapper())
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
    public ProductAssociations read() throws Exception {
        this.reader.afterPropertiesSet();
        return this.reader.read();
    }

    private static String selectClause = """            
            rs.*
            """;
    private static String fromClause = """
            (SELECT
            p.id              as rs_product_id,
            p.uuid            as product_uuid,
            p.status          as product_status,
            p.type            as product_type,
            p.subtype         as product_subtype,
            p.inventory        as product_inventory,
            p.has_online_flag as product_has_online_flag,
            p.deleted_at      as product_deleted_at,
            p.updated_at      as product_updated_at,
            subp.id           as sub_product_id,
            subp.uuid         as sub_product_uuid,
            subp.type         as sub_product_type,
            subp.subtype      as sub_product_subtype,
            subp.updated_at   as sub_product_updated_at,
            subp.deleted_at   as sub_product_deleted_at,
            pr.marsha_code    as marsha_code,
            pr.id             as property_id,
            pr.uuid           as property_uuid,
            pr.status         as property_status,
            pr.deleted_at     as property_deleted_at,
            pr.is_cp_enabled  as property_is_cp_enabled,
            pr.is_sto_enabled as property_is_sto_enabled,
            pr.updated_at     as property_updated_at,
            pp.`rank`         as product_property_rank,
            o.id              as outlet_id,
            o.uuid            as outlet_uuid,
            o.status          as outlet_status,
            o.deleted_at      as outlet_deleted_at,
            o.updated_at      as outlet_updated_at,
            co.id             as product_campaign_offer_id,
            co.uuid           as product_campaign_offer_uuid,
            co.deleted_at     as product_campaign_offer_deleted_at,
            co.updated_at     as product_campaign_offer_updated_at,
            c.id              as campaign_id,
            c.uuid            as campaign_uuid,
            c.enabled         as campaign_enabled,
            c.start_at        as campaign_start_at,
            c.end_at          as campaign_end_at,
            c.deleted_at      as campaign_deleted_at,
            c.updated_at      as campaign_updated_at,
            op.`rank`         as product_offer_rank,
            pop.`rank`        as product_promotion_rank,
            vpct.id           as product_purchase_time_id,
            vpct.uuid         as product_purchase_time_uuid,
            vpct.deleted_at   as product_purchase_time_deleted_at,
            vpct.updated_at   as product_purchase_time_updated_at,
            vpct.date         as product_purchase_time_date,
            vpct.start_time   as product_purchase_time_start_time,
            vpct.end_time     as product_purchase_time_end_time
            FROM
            products p
            left join (select id, type, subtype, status, uuid, bundle_product_id, updated_at, deleted_at
                      from products
                      where bundle_product_id is not null) subp on subp.bundle_product_id = p.id
            left join voucher_products_constraint_outlets vpco
                     on subp.id = vpco.product_id
            left join outlets o on vpco.applicable_outlet_uuid = o.uuid
            left join voucher_products_constraint_properties vpcp
                     on vpcp.product_id = subp.id
            left join properties pr on pr.marsha_code = vpco.marsha_code or pr.uuid = vpcp.applicable_property_uuid
            left join property_promotions pp
                     on pp.product_uuid = p.uuid and pp.deleted_at is null and (pp.marsha_code = vpco.marsha_code or pp.marsha_code = vpcp.marsha_code)
            left join campaign_offer_products cop on cop.product_uuid = p.uuid
            left join campaign_offers co on co.id = cop.offer_id
            left join campaign c on c.id = co.campaign_id
            left join offer_promotions op on op.product_uuid = p.uuid and co.uuid = op.offer_uuid and op.deleted_at is null
            left join product_promotions pop on pop.product_uuid = p.uuid and pop.deleted_at is null
            left join voucher_products_constraint_purchase_time vpct on vpct.product_id = p.id
            WHERE
            p.type = 'BUNDLE'
            and (p.status != 'DRAFT'
              or p.has_online_flag = 1)
            and p.bundle_product_id is null
            and (p.updated_at
                     > :startTime
                     and p.updated_at
                     < :endTime
              or p.deleted_at
                     > :startTime
                     and p.deleted_at
                     < :endTime
              or subp.updated_at
                     > :startTime
                     and subp.updated_at
                     < :endTime
              or subp.deleted_at
                     > :startTime
                     and subp.deleted_at
                     < :endTime
              or pr.updated_at
                     > :startTime
                     and pr.updated_at
                     < :endTime
              or pr.deleted_at
                     > :startTime
                     and pr.deleted_at
                     < :endTime
              or co.updated_at
                     > :startTime
                     and co.updated_at
                     < :endTime
              or co.deleted_at
                     > :startTime
                     and co.deleted_at
                     < :endTime
              or c.updated_at
                     > :startTime
                     and c.updated_at
                     < :endTime
              or c.deleted_at
                     > :startTime
                     and c.deleted_at
                     < :endTime
              or exists (select 1
                         from offer_promotions op2
                         where op2.product_uuid = p.uuid
                             and co.uuid = op2.offer_uuid
                             and op2.updated_at
                                   > :startTime
                             and
                               op2.updated_at
                                   < :endTime
                            or op2.deleted_at
                                   > :startTime
                             and op2.deleted_at
                                   < :endTime)
              or exists (select 1
                         from product_promotions pop2
                         where pop.product_uuid = p.uuid
                             and pop2.updated_at
                                   > :startTime
                             and pop2.updated_at
                                   < :endTime
                            or pop2.deleted_at
                                   > :startTime
                             and pop2.deleted_at
                                   < :endTime)
              or vpct.updated_at
                     > :startTime
                     and vpct.updated_at
                     < :endTime
              or vpct.deleted_at
                     > :startTime
                     and vpct.deleted_at
                     < :endTime
              or exists (select 1
                         from property_promotions pp2
                         where pp2.product_uuid = p.uuid
                             and
                               pp2.marsha_code = vpco.marsha_code
                             and pp2.updated_at
                                   > :startTime
                             and
                               pp.updated_at
                                   < :endTime
                            or pp2.deleted_at
                                   > :startTime
                             and pp2.deleted_at
                                   < :endTime)
              or cop.updated_at
                     > :startTime
                     and cop.updated_at
                     < :endTime
              or cop.deleted_at
                     > :startTime
                     and cop.deleted_at
                     < :endTime
              or vpco.updated_at
                     > :startTime
                     and vpco.updated_at
                     < :endTime
              or vpco.deleted_at
                     > :startTime
                     and vpco.deleted_at
                     < :endTime
              or vpcp.updated_at
                     > :startTime
                     and vpcp.updated_at
                     < :endTime
              or vpcp.deleted_at
                     > :startTime
                     and vpcp.deleted_at
                     < :endTime
              or o.updated_at
                     > :startTime
                     and o.updated_at
                     < :endTime
              or o.deleted_at
                     > :startTime
                     and o.deleted_at
                     < :endTime
              )) rs
            """;

    private Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("rs_product_id", Order.ASCENDING);
        sortKeys.put("sub_product_id", Order.ASCENDING);
        sortKeys.put("outlet_id", Order.ASCENDING);
        sortKeys.put("property_id", Order.ASCENDING);
        sortKeys.put("product_campaign_offer_id", Order.ASCENDING);
        sortKeys.put("campaign_id", Order.ASCENDING);
        sortKeys.put("product_purchase_time_id", Order.ASCENDING);
        return sortKeys;
    }
}
