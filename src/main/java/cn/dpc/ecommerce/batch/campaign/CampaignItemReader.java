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
public class CampaignItemReader implements ItemReader<Campaign> {
    private final JdbcPagingItemReader<Campaign> reader;

    public CampaignItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        this.reader = new JdbcPagingItemReaderBuilder<Campaign>()
                .name("campaignItemReader")
                .dataSource(dataSource)
                .rowMapper(Campaign.getRowMapper())
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
    public Campaign read() throws Exception {
        this.reader.afterPropertiesSet();
        return this.reader.read();
    }

    private static final String selectClause = "rs.*";

    private static final String fromClause = """
            (select
            c.id                             as campaign_id,
            c.uuid                           as campaign_uuid,
            c.enabled                        as campaign_enabled,
            c.name                           as campaign_name,
            c.subtitle                       as campaign_subtitle,
            c.type                           as campaign_type,
            c.warm_up_type                   as campaign_warm_up_type,
            c.start_at                       as campaign_start_at,
            c.end_at                         as campaign_end_at,
            c.online_at                      as campaign_online_at,
            c.description                    as campaign_description,
            c.deleted_at                     as campaign_deleted_at,
            c.updated_at                     as campaign_updated_at,
            c.warm_up                        as campaign_warm_up,
            case
                when IFNULL(c.image, '') != '' then c.image
                else campaign_contents.image end as campaign_image,
            case
                when c.warm_up_type = 'IMMEDIATE' then c.online_at
                else DATE_SUB(c.start_at, INTERVAL IFNULL(c.warm_up, 0) hour) end as campaign_warm_up_date,
            (select GROUP_CONCAT(ct.name)
                from campaign_tags ct
                where ct.campaign_id = c.id
                and ct.deleted_at is null)   as campaign_tag_names,
            (select JSON_ARRAYAGG(JSON_OBJECT('name', ct.name, 'uuid', ct.tag_uuid, 'type', 'CAMPAIGN'))
                from campaign_tags ct
                where ct.campaign_id = c.id
                and ct.deleted_at is null)   as campaign_tags
            from campaign c
            left join campaign_contents
                on campaign_contents.campaign_id = c.id
                and campaign_contents.position = 'HEADER'
                and (campaign_contents.type = 'VIDEO' or campaign_contents.type = 'IMAGE')
                and campaign_contents.rank = 1
                and campaign_contents.image is not null
                and campaign_contents.deleted_at is null
                and campaign_contents.enabled = 1
            where c.updated_at > :startTime and c.updated_at < :endTime
               or c.deleted_at > :startTime and c.deleted_at < :endTime
               or exists (select 1
                          from campaign_contents cc
                          where c.id = cc.campaign_id and cc.updated_at > :startTime and cc.updated_at < :endTime
                             or cc.deleted_at > :startTime and cc.deleted_at < :endTime)
               or exists (select 1
                          from campaign_tags tag
                          where tag.campaign_id = c.id
                              and tag.updated_at > :startTime and tag.updated_at < :endTime
                             or tag.deleted_at > :startTime and tag.deleted_at < :endTime)) rs
            """;

    private Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("campaign_id", Order.ASCENDING);
        return sortKeys;
    }

}
