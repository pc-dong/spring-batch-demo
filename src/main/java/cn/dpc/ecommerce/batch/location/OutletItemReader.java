package cn.dpc.ecommerce.batch.location;

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
public class OutletItemReader extends BaseItemReader<Outlet> {
    public OutletItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        super(new JdbcPagingItemReaderBuilder<Outlet>()
                .name("outletItemReader")
                .dataSource(dataSource)
                .rowMapper(Outlet.getRowMapper())
                .selectClause(selectClause)
                .fromClause(fromClause)
                .sortKeys(getSortKeys())
                .pageSize(pageSize)
                .fetchSize(fetchSize)
                .build());
    }

    private static String selectClause = """
            o.*
            """;

    private static String fromClause = """
            (select
            distinct o.id as outlet_id,
            o.uuid as outlet_uuid,
            o.name_chinese as outlet_name_chinese,
            o.name_english as outlet_name_english,
            o.type as outlet_type,
            o.status as outlet_status,
            o.service as outlet_service,
            o.`rank` as outlet_rank,
            o.updated_at as outlet_updated_at,
            (select JSON_ARRAYAGG(JSON_OBJECT('day_of_week', oh.day_of_week, 'opening_at', oh.opening_at, 'closing_at', closing_at)) from outlets_opening_hours oh where outlet_id = o.id and oh.deleted_at is null)
                as outlet_opening_hours,
            o.arrangement_chinese as outlet_arrangement_chinese,
            o.arrangement_english as outlet_arrangement_english,
            o.details_chinese as outlet_details_chinese,
            o.details_english as outlet_details_english,
            o.media_type as outlet_media_type,
            o.video_cover_image_url as outlet_video_cover_image_url,
            o.cover_image_url as outlet_cover_image_url,
            (select JSON_ARRAYAGG(oi.image_url) from outlets_banner_images oi where oi.outlet_id = o.id and oi.deleted_at is null) as outlet_banner_images_urls,
                o.address as outlet_address,
            o.short_description_chinese as outlet_short_description_chinese,
            o.mobile_phone as outlet_mobile_phone,
            o.mobile_country_code as outlet_mobile_country_code,
            o.landline as outlet_landline,
            o.region_code as outlet_region_code,
            o.landline_country_code as outlet_landline_country_code,
            o.landline_extension as outlet_landline_extension,
            o.online_booking_flag as outlet_online_booking_flag,
            o.deleted_at as outlet_deleted_at,
            p.id as property_id,
            p.uuid as property_uuid,
            p.updated_at as property_updated_at,
            p.deleted_at as property_deleted_at,
            p.is_reservation_enabled as property_is_reservation_enabled,
            p.is_cp_enabled as property_is_cp_enabled,
            p.status as property_status
            from outlets o
            inner join properties p on o.property_id = p.id
            where o.updated_at > :startTime and o.updated_at < :endTime
            or p.updated_at > :startTime and p.updated_at < :endTime
            or p.deleted_at > :startTime and p.deleted_at < :endTime
            or exists(
                select 1
                from outlets_opening_hours oh
                where oh.outlet_id = o.id
                and oh.updated_at > :startTime and oh.updated_at < :endTime
                or oh.deleted_at > :startTime and oh.deleted_at < :endTime)
            or exists(
                select 1
                from outlets_banner_images oi
                where oi.outlet_id = o.id
                and oi.updated_at > :startTime and oi.updated_at < :endTime
                or oi.deleted_at > :startTime and oi.deleted_at < :endTime)
            ) as o
            """;

    private static Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("outlet_id", Order.ASCENDING);
        return sortKeys;
    }

}
