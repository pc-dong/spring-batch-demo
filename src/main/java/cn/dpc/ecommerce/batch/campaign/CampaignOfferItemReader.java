package cn.dpc.ecommerce.batch.campaign;

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
public class CampaignOfferItemReader implements ItemReader<CampaignOffer> {
    private final JdbcPagingItemReader<CampaignOffer> reader;

    public CampaignOfferItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        this.reader = new JdbcPagingItemReaderBuilder<CampaignOffer>()
                .name("campaignOfferItemReader")
                .dataSource(dataSource)
                .rowMapper(CampaignOffer.getRowMapper())
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
    public CampaignOffer read() throws Exception {
        this.reader.afterPropertiesSet();
        return this.reader.read();
    }

    private static final String selectClause = "rs.*";

    private static String fromClause = """
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

    private Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("campaign_offer_id", Order.ASCENDING);
        return sortKeys;
    }

}
