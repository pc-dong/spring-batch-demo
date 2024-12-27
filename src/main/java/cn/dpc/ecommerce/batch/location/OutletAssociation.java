package cn.dpc.ecommerce.batch.location;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;
import static cn.dpc.ecommerce.batch.consts.Constants.NULL_UUID;
import static cn.dpc.ecommerce.batch.consts.Constants.activePropertyStatus;

@Data
@Accessors(chain = true)
public class OutletAssociation {
    private Long outlet_id;
    private String outlet_uuid;
    private String outlet_status;
    private LocalDateTime outlet_deleted_at;
    private LocalDateTime outlet_updated_at;
    private String outlet_cuisine_uuid;
    private Long outlet_cuisine_id;
    private LocalDateTime outlet_cuisine_deleted_at;
    private LocalDateTime outlet_cuisine_updated_at;
    private Long cuisine_id;
    private String cuisine_uuid;
    private String cuisine_name_chinese;
    private LocalDateTime cuisine_deleted_at;
    private LocalDateTime cuisine_updated_at;
    private Long property_id;
    private String property_uuid;
    private LocalDateTime property_deleted_at;
    private LocalDateTime property_updated_at;
    private Integer property_is_cp_enabled;
    private String property_status;

    public String getOutlet_uuid() {
        return Optional.ofNullable(outlet_uuid).orElse(NULL_UUID);
    }

    public Long getCuisine_id() {
        return Optional.ofNullable(cuisine_id).orElse(NULL_ID);
    }

    public String getCuisine_uuid() {
        return Optional.ofNullable(cuisine_uuid).orElse(NULL_UUID);
    }

    public String getCuisine_name_chinese() {
        return Optional.ofNullable(cuisine_name_chinese).orElse("");
    }

    public String getProperty_uuid() {
        return Optional.ofNullable(property_uuid).orElse(NULL_UUID);
    }

    public static RowMapper<OutletAssociation> getRowMapper() {
        return (rs, rowNum) -> new OutletAssociation()
                .setOutlet_id(rs.getLong("outlet_id"))
                .setOutlet_uuid(Optional.ofNullable(rs.getString("outlet_uuid")).orElse(NULL_UUID))
                .setOutlet_status(Optional.ofNullable(rs.getString("outlet_status")).orElse(null))
                .setOutlet_deleted_at(Optional.ofNullable(rs.getTimestamp("outlet_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setOutlet_updated_at(Optional.ofNullable(rs.getTimestamp("outlet_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setOutlet_cuisine_uuid(Optional.ofNullable(rs.getString("outlet_cuisine_uuid")).orElse(NULL_UUID))
                .setOutlet_cuisine_id(rs.getLong("outlet_cuisine_id"))
                .setOutlet_cuisine_deleted_at(Optional.ofNullable(rs.getTimestamp("outlet_cuisine_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setOutlet_cuisine_updated_at(Optional.ofNullable(rs.getTimestamp("outlet_cuisine_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCuisine_id(rs.getLong("cuisine_id"))
                .setCuisine_uuid(Optional.ofNullable(rs.getString("cuisine_uuid")).orElse(NULL_UUID))
                .setCuisine_name_chinese(Optional.ofNullable(rs.getString("cuisine_name_chinese")).orElse(""))
                .setCuisine_deleted_at(Optional.ofNullable(rs.getTimestamp("cuisine_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCuisine_updated_at(Optional.ofNullable(rs.getTimestamp("cuisine_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProperty_id(rs.getLong("property_id"))
                .setProperty_uuid(Optional.ofNullable(rs.getString("property_uuid")).orElse(NULL_UUID))
                .setProperty_deleted_at(Optional.ofNullable(rs.getTimestamp("property_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProperty_updated_at(Optional.ofNullable(rs.getTimestamp("property_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProperty_is_cp_enabled(rs.getInt("is_cp_enabled"))
                .setProperty_status(Optional.ofNullable(rs.getString("property_status")).orElse(null));
    }

    public boolean shouldPropertyDeleted() {
        return NULL_ID != property_id
                && (property_deleted_at != null
                || !activePropertyStatus.contains(property_status));
    }

    public boolean shouldOutletDeleted() {
        return NULL_ID != outlet_id
                && (shouldPropertyDeleted()
                || outlet_deleted_at != null
                || !"VISIBLE".equals(outlet_status));
    }

    public boolean shouldCuisineDeleted() {
        return NULL_ID != cuisine_id
                && (cuisine_deleted_at != null
                || outlet_cuisine_deleted_at != null);
    }

    public boolean shouldDeleted() {
        return shouldCuisineDeleted()
                || shouldOutletDeleted();
    }

    public String getAssociationId() {
        return getAssociationId(getOutlet_id(), getCuisine_id());
    }

    public String getAssociationId(Long outletId, Long cuisineId) {
        // O_{outlet-id}_{cuisine_id}
        return String.format("O_%d_%d", outletId, cuisineId);
    }

    public List<String> getShouldDeletedIdsForUpdate() {
        return getShouldDeletedIdsForDelete();
    }

    public LocalDateTime getUpdatedAt() {
        return Stream.of(outlet_updated_at,
                        outlet_deleted_at,
                        property_deleted_at,
                        property_updated_at,
                        cuisine_updated_at,
                        cuisine_deleted_at
                )
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(null);
    }

    public List<String> getShouldDeletedIdsForDelete() {
        return NULL_ID == getCuisine_id() ? List.of() :  List.of(getAssociationId(outlet_id, NULL_ID));
    }
}
