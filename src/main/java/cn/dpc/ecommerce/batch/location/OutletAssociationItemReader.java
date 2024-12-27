package cn.dpc.ecommerce.batch.location;

import cn.dpc.ecommerce.batch.common.BaseItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class OutletAssociationItemReader extends BaseItemReader<OutletAssociation> {

    public OutletAssociationItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        super(new JdbcPagingItemReaderBuilder<OutletAssociation>()
                .name("productAssociationItemReader")
                .dataSource(dataSource)
                .rowMapper(OutletAssociation.getRowMapper())
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
            (select o.id            as outlet_id,
                    o.uuid          as outlet_uuid,
                    o.status        as outlet_status,
                    o.deleted_at    as outlet_deleted_at,
                    o.updated_at    as outlet_updated_at,
                    oc.uuid         as outlet_cuisine_uuid,
                    oc.id           as outlet_cuisine_id,
                    oc.deleted_at   as outlet_cuisine_deleted_at,
                    oc.updated_at   as outlet_cuisine_updated_at,
                    c.id            as cuisine_id,
                    c.uuid          as cuisine_uuid,
                    c.deleted_at    as cuisine_deleted_at,
                    c.updated_at    as cuisine_updated_at,
                    c.name_chinese  as cuisine_name_chinese,
                    p.id            as property_id,
                    p.uuid          as property_uuid,
                    p.updated_at   as property_updated_at,
                    p.deleted_at    as property_deleted_at,
                    p.is_cp_enabled as is_cp_enabled,
                    p.status        as property_status
             from outlets o
                      left join outlets_outlet_cuisines oc on o.id = oc.outlet_id
                      left join outlet_cuisines c on oc.cuisine_id = c.id
                      left join properties p on o.property_id = p.id
             where o.updated_at > :startTime and o.updated_at < :endTime
                or o.deleted_at > :startTime and o.deleted_at < :endTime
                or c.updated_at > :startTime and c.updated_at < :endTime
                or c.deleted_at > :startTime and c.deleted_at < :endTime
                or oc.updated_at > :startTime and oc.updated_at < :endTime
                or oc.deleted_at > :startTime and oc.deleted_at < :endTime
                or p.updated_at > :startTime and p.updated_at < :endTime
                or p.deleted_at > :startTime and p.deleted_at < :endTime) pa
            """;

    private static Map<String, Order> getSortKeys() {
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("outlet_id", Order.ASCENDING);
        sortKeys.put("cuisine_id", Order.ASCENDING);
        return sortKeys;
    }
}
