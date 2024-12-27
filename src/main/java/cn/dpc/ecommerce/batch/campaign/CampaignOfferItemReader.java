package cn.dpc.ecommerce.batch.campaign;

import cn.dpc.ecommerce.batch.common.BaseItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class CampaignOfferItemReader extends BaseItemReader<CampaignOffer> {
    public CampaignOfferItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        super(new JdbcPagingItemReaderBuilder<CampaignOffer>()
                .name("campaignOfferItemReader")
                .dataSource(dataSource)
                .rowMapper(CampaignOffer.getRowMapper())
                .selectClause(selectClause)
                .fromClause(fromClause)
                .sortKeys(getSortKeys())
                .pageSize(pageSize)
                .fetchSize(fetchSize)
                .build());
    }

    private static final String selectClause = "rs.*";

    private static final String fromClause = """
            (select co.id as campaign_offer_id,
                 co.uuid as campaign_offer_uuid,
                 co.name as campaign_offer_name,
                 c.enabled as campaign_enabled,
                 c.deleted_at as campaign_deleted_at,
                 c.end_at as campaign_end_at,
                 co.updated_at as campaign_offer_updated_at,
                 co.deleted_at as campaign_offer_deleted_at
            from campaign_offers co
            left join campaign c on c.id = co.campaign_id
            where co.updated_at > :startTime and co.updated_at < :endTime
            or co.deleted_at > :startTime and co.deleted_at < :endTime
            or c.updated_at > :startTime and c.updated_at < :endTime
            or c.deleted_at > :startTime and c.deleted_at < :endTime) rs
            """;

    private static Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("campaign_offer_id", Order.ASCENDING);
        return sortKeys;
    }
}
