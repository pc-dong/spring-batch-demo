package cn.dpc.ecommerce.batch.product;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;
import static cn.dpc.ecommerce.batch.consts.Constants.NULL_UUID;
import static cn.dpc.ecommerce.batch.consts.Constants.activePropertyStatus;

@Data
@Accessors(chain = true)
public class ProductAssociations {
    private Long product_id;
    private String product_uuid;
    private String product_status;
    private String product_type;
    private Long product_inventory;
    private String product_subtype;
    private Boolean product_has_online_flag;
    private LocalDateTime product_deleted_at;
    private LocalDateTime product_updated_at;
    private Long sub_product_id;
    private String sub_product_uuid;
    private String sub_product_type;
    private String sub_product_subtype;
    private LocalDateTime sub_product_updated_at;
    private LocalDateTime sub_product_deleted_at;
    private String marsha_code;
    private Long property_id;
    private String property_uuid;
    private String property_status;
    private LocalDateTime property_deleted_at;
    private Boolean property_is_cp_enabled;
    private Boolean property_is_sto_enabled;
    private LocalDateTime property_updated_at;
    private Integer product_property_rank;
    private Long outlet_id;
    private String outlet_uuid;
    private String outlet_status;
    private LocalDateTime outlet_deleted_at;
    private LocalDateTime outlet_updated_at;
    private Long product_campaign_offer_id;
    private String product_campaign_offer_uuid;
    private LocalDateTime product_campaign_offer_deleted_at;
    private LocalDateTime product_campaign_offer_updated_at;
    private Long campaign_id;
    private String campaign_uuid;
    private Boolean campaign_enabled;
    private LocalDateTime campaign_start_at;
    private LocalDateTime campaign_end_at;
    private LocalDateTime campaign_deleted_at;
    private LocalDateTime campaign_updated_at;
    private Integer product_offer_rank;
    private Integer product_promotion_rank;
    private Long product_purchase_time_id;
    private String product_purchase_time_uuid;
    private LocalDateTime product_purchase_time_deleted_at;
    private LocalDateTime product_purchase_time_updated_at;
    private LocalDate product_purchase_time_date;
    private LocalTime product_purchase_time_start_time;
    private LocalTime product_purchase_time_end_time;

    public Long getProduct_id() {
        return Optional.ofNullable(product_id).orElse(NULL_ID);
    }

    public Long getSub_product_id() {
        return Optional.ofNullable(sub_product_id).orElse(NULL_ID);
    }

    public Long getProperty_id() {
        return Optional.ofNullable(property_id).orElse(NULL_ID);
    }

    public Long getOutlet_id() {
        return Optional.ofNullable(outlet_id).orElse(NULL_ID);
    }

    public Long getProduct_campaign_offer_id() {
        return Optional.ofNullable(product_campaign_offer_id).orElse(NULL_ID);
    }

    public Long getCampaign_id() {
        return Optional.ofNullable(campaign_id).orElse(NULL_ID);
    }

    public String getProduct_uuid() {
        return Optional.ofNullable(product_uuid).orElse(NULL_UUID);
    }

    public String getSub_product_uuid() {
        return Optional.ofNullable(sub_product_uuid).orElse(NULL_UUID);
    }

    public String getProperty_uuid() {
        return Optional.ofNullable(property_uuid).orElse(NULL_UUID);
    }

    public String getOutlet_uuid() {
        return Optional.ofNullable(outlet_uuid).orElse(NULL_UUID);
    }

    public String getProduct_campaign_offer_uuid() {
        return Optional.ofNullable(product_campaign_offer_uuid).orElse(NULL_UUID);
    }

    public String getCampaign_uuid() {
        return Optional.ofNullable(campaign_uuid).orElse(NULL_UUID);
    }

    public String getProduct_purchase_time_uuid() {
        return Optional.ofNullable(product_purchase_time_uuid).orElse(NULL_UUID);
    }

    public Long getOutletOrPropertyId() {
        if (this.getProduct_type().equals("FB") || "FB".equals(this.getSub_product_type())) {
            return getOutlet_id();
        }
        return getProperty_id();
    }

    public String getAssociationId() {
        return getAssociationId(getProduct_id(), getSub_product_id(), getOutletOrPropertyId(), getProduct_purchase_time_id(), getProduct_campaign_offer_id());
    }

    public static String getAssociationId(Long productId, Long subProductId, Long propertyOrOutletId, Long purchaseTimeId, Long campaignOfferId) {
        // P_{product_id}_{sub_product_id}_{property_id/outlet_id}_{purchase_time_id}_{campaign_offer_id}
        String format = "P_%s_%s_%s_%s_%s";
        return String.format(format, productId, subProductId, propertyOrOutletId, purchaseTimeId, campaignOfferId);
    }

    public Integer getProduct_promotion_rank() {
        return Optional.ofNullable(product_promotion_rank).orElse(0);
    }

    public Integer getProduct_offer_rank() {
        return Optional.ofNullable(product_offer_rank).orElse(0);
    }

    public Integer getProduct_property_rank() {
        return Optional.ofNullable(product_property_rank).orElse(0);
    }

    public boolean shouldProductDeleted() {
        return product_deleted_at != null || !"ONLINE".equals(product_status) || product_inventory <= 0
                || shouldPropertyDeleted() || shouldOutletDeleted();
    }

    public boolean shouldSubProductDeleted() {
        return NULL_ID != getSub_product_id() && (sub_product_deleted_at != null);
    }

    public boolean shouldPropertyDeleted() {
        return NULL_ID != getProperty_id()
                && (property_deleted_at != null
                || !activePropertyStatus.contains(property_status));
    }

    public boolean shouldOutletDeleted() {
        return NULL_ID != getOutlet_id()
                && (shouldPropertyDeleted()
                || outlet_deleted_at != null
                || "INVISIBLE".equals(outlet_status));
    }

    public boolean shouldCampaignDeleted() {
        return NULL_ID != getCampaign_id()
                &&
                (campaign_deleted_at != null
                        || !campaign_enabled
                        || Optional.ofNullable(campaign_end_at)
                        .map(end -> end.isBefore(LocalDateTime.now()))
                        .orElse(false));
    }

    public boolean shouldCampaignOfferDeleted() {
        return NULL_ID != getProduct_campaign_offer_id()
                && product_campaign_offer_deleted_at != null;
    }

    public boolean shouldProductPurchaseTimeDeleted() {
        return NULL_ID != getProduct_purchase_time_id()
                && product_purchase_time_deleted_at != null;
    }

    public boolean shouldDeleted() {
        return shouldProductDeleted()
                || shouldSubProductDeleted()
                || shouldPropertyDeleted()
                || shouldOutletDeleted()
                || shouldCampaignDeleted()
                || shouldCampaignOfferDeleted()
                || shouldProductPurchaseTimeDeleted();
    }


    public static RowMapper<ProductAssociations> getProductAssociationsRowMapper() {
        return (rs, rowNum) -> new ProductAssociations()
                .setProduct_id(rs.getLong("rs_product_id"))
                .setProduct_uuid(rs.getString("product_uuid"))
                .setProduct_status(rs.getString("product_status"))
                .setProduct_type(rs.getString("product_type"))
                .setProduct_subtype(rs.getString("product_subtype"))
                .setProduct_inventory(rs.getLong("product_inventory"))
                .setProduct_has_online_flag(rs.getBoolean("product_has_online_flag"))
                .setProduct_deleted_at(Optional.ofNullable(rs.getTimestamp("product_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_updated_at(Optional.ofNullable(rs.getTimestamp("product_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setSub_product_id(rs.getLong("sub_product_id"))
                .setSub_product_uuid(rs.getString("sub_product_uuid"))
                .setSub_product_type(rs.getString("sub_product_type"))
                .setSub_product_subtype(rs.getString("sub_product_subtype"))
                .setSub_product_updated_at(Optional.ofNullable(rs.getTimestamp("sub_product_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setSub_product_deleted_at(Optional.ofNullable(rs.getTimestamp("sub_product_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setMarsha_code(rs.getString("marsha_code"))
                .setProperty_id(rs.getLong("property_id"))
                .setProperty_uuid(rs.getString("property_uuid"))
                .setProperty_status(rs.getString("property_status"))
                .setProperty_deleted_at(Optional.ofNullable(rs.getTimestamp("property_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProperty_is_cp_enabled(rs.getBoolean("property_is_cp_enabled"))
                .setProperty_is_sto_enabled(rs.getBoolean("property_is_sto_enabled"))
                .setProperty_updated_at(Optional.ofNullable(rs.getTimestamp("property_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_property_rank(rs.getInt("product_property_rank"))
                .setOutlet_id(rs.getLong("outlet_id"))
                .setOutlet_uuid(rs.getString("outlet_uuid"))
                .setOutlet_status(rs.getString("outlet_status"))
                .setOutlet_deleted_at(Optional.ofNullable(rs.getTimestamp("outlet_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setOutlet_updated_at(Optional.ofNullable(rs.getTimestamp("outlet_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_campaign_offer_id(rs.getLong("product_campaign_offer_id"))
                .setProduct_campaign_offer_uuid(rs.getString("product_campaign_offer_uuid"))
                .setProduct_campaign_offer_deleted_at(Optional.ofNullable(rs.getTimestamp("product_campaign_offer_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_campaign_offer_updated_at(Optional.ofNullable(rs.getTimestamp("product_campaign_offer_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_id(rs.getLong("campaign_id"))
                .setCampaign_uuid(rs.getString("campaign_uuid"))
                .setCampaign_enabled(rs.getBoolean("campaign_enabled"))
                .setCampaign_start_at(Optional.ofNullable(rs.getTimestamp("campaign_start_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_end_at(Optional.ofNullable(rs.getTimestamp("campaign_end_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_deleted_at(Optional.ofNullable(rs.getTimestamp("campaign_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_updated_at(Optional.ofNullable(rs.getTimestamp("campaign_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_offer_rank(rs.getInt("product_offer_rank"))
                .setProduct_promotion_rank(rs.getInt("product_promotion_rank"))
                .setProduct_purchase_time_id(rs.getLong("product_purchase_time_id"))
                .setProduct_purchase_time_uuid(rs.getString("product_purchase_time_uuid"))
                .setProduct_purchase_time_deleted_at(Optional.ofNullable(rs.getTimestamp("product_purchase_time_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_purchase_time_updated_at(Optional.ofNullable(rs.getTimestamp("product_purchase_time_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_purchase_time_date(Optional.ofNullable(rs.getDate("product_purchase_time_date")).map(Date::toLocalDate).orElse(null))
                .setProduct_purchase_time_start_time(Optional.ofNullable(rs.getTime("product_purchase_time_start_time")).map(Time::toLocalTime).orElse(null))
                .setProduct_purchase_time_end_time(Optional.ofNullable(rs.getTime("product_purchase_time_end_time")).map(Time::toLocalTime).orElse(null));
    }

    public Optional<LocalDateTime> getPurchase_start_time() {
        if (null != product_purchase_time_date && null != product_purchase_time_start_time) {
            return Optional.of(LocalDateTime.of(product_purchase_time_date, product_purchase_time_start_time));
        }

        return Optional.empty();
    }

    public Optional<LocalDateTime> getPurchase_end_time() {
        if (null != product_purchase_time_date && null != product_purchase_time_end_time) {
            return Optional.of(LocalDateTime.of(product_purchase_time_date, product_purchase_time_end_time));
        }

        return Optional.empty();
    }

    public LocalDateTime getUpdatedAt() {
        return Stream.of(product_updated_at, product_deleted_at,
                        sub_product_updated_at, sub_product_deleted_at,
                        property_updated_at, property_deleted_at,
                        outlet_updated_at, outlet_deleted_at,
                        product_campaign_offer_updated_at, product_campaign_offer_deleted_at,
                        campaign_updated_at, campaign_deleted_at,
                        product_purchase_time_updated_at, product_purchase_time_deleted_at)
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(null);
    }

    public static final String SUB_PRODUCT = "sub_product";
    public static final String PROPERTY = "property";
    public static final String OUTLET = "outlet";
    public static final String CAMPAIGN = "campaign";
    public static final String CAMPAIGN_OFFER = "campaign_offer";
    public static final String PURCHASE_TIME = "purchase_time";

    public List<String> getShouldDeletedIdsForDelete() {
        List<String> ids = new ArrayList<>();
        ids.addAll(getShouldDeletedIdsForSubProductDelete());
        ids.addAll(getShouldDeletedIdsForPropertyOrOutletDelete());
        ids.addAll(getShouldDeletedIdsForPurchaseTimeDelete());
        ids.addAll(getShouldDeletedIdsForCampaignDelete());
        return ids;
    }

    public List<String> getShouldDeletedIdsForSubProductDelete() {
        List<String> ids = new ArrayList<>();
        if (NULL_ID != getSub_product_id()) {
            ids.add(getAssociationId(getProduct_id(), NULL_ID, getOutletOrPropertyId(), getProduct_purchase_time_id(), getProduct_campaign_offer_id()));
            ids.add(getAssociationId(getProduct_id(), NULL_ID, NULL_ID, getProduct_purchase_time_id(), getProduct_campaign_offer_id()));
            ids.add(getAssociationId(getProduct_id(), NULL_ID, getOutletOrPropertyId(), NULL_ID, getProduct_campaign_offer_id()));
            ids.add(getAssociationId(getProduct_id(), NULL_ID, getOutletOrPropertyId(), getProduct_purchase_time_id(), NULL_ID));
            ids.add(getAssociationId(getProduct_id(), NULL_ID, NULL_ID, NULL_ID, getProduct_campaign_offer_id()));
            ids.add(getAssociationId(getProduct_id(), NULL_ID, NULL_ID, getProduct_purchase_time_id(), NULL_ID));
            ids.add(getAssociationId(getProduct_id(), NULL_ID, getOutletOrPropertyId(), NULL_ID, NULL_ID));
            ids.add(getAssociationId(getProduct_id(), NULL_ID, NULL_ID, NULL_ID, NULL_ID));
        }
        return ids;
    }

    public List<String> getShouldDeletedIdsForPropertyOrOutletDelete() {
        List<String> ids = new ArrayList<>();
        if (NULL_ID != getOutletOrPropertyId()) {
            ids.add(getAssociationId(getProduct_id(), getSub_product_id(), NULL_ID, getProduct_purchase_time_id(), getProduct_campaign_offer_id()));
            ids.add(getAssociationId(getProduct_id(), getSub_product_id(), NULL_ID, getProduct_purchase_time_id(), NULL_ID));
            ids.add(getAssociationId(getProduct_id(), getSub_product_id(), NULL_ID, NULL_ID, getProduct_campaign_offer_id()));
            ids.add(getAssociationId(getProduct_id(), getSub_product_id(), NULL_ID, NULL_ID, NULL_ID));
        }
        return ids;
    }

    public List<String> getShouldDeletedIdsForPurchaseTimeDelete() {
        List<String> ids = new ArrayList<>();
        if (NULL_ID != getProduct_purchase_time_id()) {
            ids.add(getAssociationId(getProduct_id(), getSub_product_id(), getOutletOrPropertyId(), NULL_ID, getProduct_campaign_offer_id()));
            ids.add(getAssociationId(getProduct_id(), getSub_product_id(), getOutletOrPropertyId(), NULL_ID, NULL_ID));
        }
        return ids;
    }

    public List<String> getShouldDeletedIdsForCampaignDelete() {
        List<String> ids = new ArrayList<>();
        if (NULL_ID != getProduct_campaign_offer_id()) {
            ids.add(getAssociationId(getProduct_id(), getSub_product_id(), getOutletOrPropertyId(), getProduct_purchase_time_id(), NULL_ID));
        }
        return ids;
    }


    public List<String> getShouldDeletedIdsForUpdate() {
        return getShouldDeletedIdsForDelete();
    }
}
