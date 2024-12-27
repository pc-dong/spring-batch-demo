package cn.dpc.ecommerce.batch.campaign;

import cn.dpc.ecommerce.batch.common.BaseItemReader;
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
public class CampaignAssociationItemReader extends BaseItemReader<CampaignAssociation> {

    public CampaignAssociationItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        super(new JdbcPagingItemReaderBuilder<CampaignAssociation>()
                .name("productAssociationItemReader")
                .dataSource(dataSource)
                .rowMapper(CampaignAssociation.getRowMapper())
                .selectClause(selectClause)
                .fromClause(fromClause)
                .sortKeys(getSortKeys())
                .pageSize(pageSize)
                .fetchSize(fetchSize)
                .build());
    }

    private static final String selectClause = """
            pa.*
            """;
    private static final String fromClause = """
            (select
                distinct
                c.id as campaign_id,
                c.uuid as campaign_uuid,
                c.start_at as campaign_start_at,
                c.end_at as campaign_end_at,
                c.enabled as campaign_enabled,
                c.updated_at as campaign_updated_at,
                c.deleted_at as campaign_deleted_at,
                co.id as campaign_offer_id,
                co.uuid as campaign_offer_uuid,
                co.updated_at as campaign_offer_updated_at,
                co.deleted_at as campaign_offer_deleted_at,
                (select count(1) from campaign_offer_products cop where campaign_offer_id = co.id and cop.deleted_at is null)
                    as campaign_offer_products_count
            from campaign c
                     left join campaign_offers co on c.id = co.campaign_id
            where
               c.updated_at > :startTime and c.updated_at < :endTime
               or c.deleted_at > :startTime and c.deleted_at < :endTime
               or co.updated_at > :startTime and co.updated_at < :endTime
               or co.deleted_at > :startTime and co.deleted_at < :endTime
               or exists (select 1 from campaign_offer_products cop where cop.offer_id = co.id and cop.updated_at > :startTime and cop.updated_at < :endTime
                                                                       or cop.deleted_at > :startTime and cop.deleted_at < :endTime)
            ) pa
            """;

    private static Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("campaign_id", Order.ASCENDING);
        sortKeys.put("campaign_offer_id", Order.ASCENDING);
        return sortKeys;
    }
}
