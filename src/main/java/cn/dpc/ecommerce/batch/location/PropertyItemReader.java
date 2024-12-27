package cn.dpc.ecommerce.batch.location;

import cn.dpc.ecommerce.batch.common.BaseItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class PropertyItemReader extends BaseItemReader<Property> {

    public PropertyItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        super(new JdbcPagingItemReaderBuilder<Property>()
                .name("propertyItemReader")
                .dataSource(dataSource)
                .rowMapper(Property.getRowMapper())
                .selectClause(selectClause)
                .fromClause(fromClause)
                .sortKeys(getSortKeys())
                .pageSize(pageSize)
                .fetchSize(fetchSize)
                .build());
    }

    private static final String selectClause = """
            p.*
            """;

    private static final String fromClause = """
            (select
            distinct p.id            as property_id,
            p.uuid                   as property_uuid,
            p.status                 as pr_status,
            p.brand_id               as pr_brand_id,
            b.brand_code             as pr_brand_code,
            b.sub_brand              as pr_sub_brand,
            b.name_chinese           as pr_brand_name_chinese,
            bc.name_chinese          as pr_brand_category_name_chinese,
            bc.name_english          as pr_brand_category_name_english,
            bc.sorting               as pr_brand_category_sorting,
            b.sorting                as pr_brand_sorting,
            b.link_color             as pr_brand_link_color,
            b.primary_color          as pr_brand_primary_color,
            b.logo_url               as pr_brand_logo_url,
            b.name_english           as pr_brand_name_english,
            p.city_code              as pr_city_code,
            c.title_chinese          as pr_city_title_chinese,
            c.title_english          as pr_city_title_english,
            c.title_pinyin           as pr_city_title_pinyin,
            p.marsha_code            as pr_marsha_code,
            p.address_chinese        as pr_address_chinese,
            p.address_english        as pr_address_english,
            p.name_chinese           as pr_name_chinese,
            p.province_name          as pr_province_name,
            p.city_name              as pr_city_name,
            p.district_name          as pr_district_name,
            p.details_chinese        as pr_details_chinese,
            p.details_english        as pr_details_english,
            p.header_image_url       as pr_header_image_url,
            p.is_reservation_enabled as pr_is_reservation_enabled,
            p.is_cp_enabled          as pr_is_cp_enabled,
            p.deleted_at             as pr_deleted_at,
            p.updated_at             as pr_updated_at
            from properties p
            left join cities c on p.city_code = c.city_code and c.deleted_at is null
            left join brands b on p.brand_id = b.id and b.deleted_at is null
            left join brands_categories bc on b.category_id = bc.id and bc.deleted_at is null
            where p.updated_at > :startTime and p.updated_at < :endTime
            or p.deleted_at > :startTime and p.deleted_at < :endTime
            or exists(select 1
                from cities c
                where c.city_code = p.city_code and
                c.updated_at > :startTime and c.updated_at < :endTime
                or c.deleted_at > :startTime and c.deleted_at < :endTime)
            or exists(select 1
                from brands b
                inner join brands_categories bc on b.category_id = bc.id
                where b.id = p.brand_id and
                b.updated_at > :startTime and b.updated_at < :endTime
                or b.deleted_at > :startTime and b.deleted_at < :endTime
                or bc.updated_at > :startTime and bc.updated_at < :endTime
                or bc.deleted_at > :startTime and bc.deleted_at < :endTime)) as p
            """;

    private static Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("property_id", Order.ASCENDING);
        return sortKeys;
    }

}
