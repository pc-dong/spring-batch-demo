package cn.dpc.ecommerce.batch.campaign;

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

@Data
@Accessors(chain = true)
public class CampaignAssociation {
    private Long campaign_id;
    private String campaign_uuid;
    private LocalDateTime campaign_start_at;
    private LocalDateTime campaign_end_at;
    private Integer campaign_enabled;
    private LocalDateTime campaign_updated_at;
    private LocalDateTime campaign_deleted_at;
    private Long campaign_offer_id;
    private String campaign_offer_uuid;
    private LocalDateTime campaign_offer_updated_at;
    private LocalDateTime campaign_offer_deleted_at;
    private Long campaign_offer_products_count;

    public String getCampaign_uuid() {
        return Optional.ofNullable(campaign_uuid).orElse(NULL_UUID);
    }

    public String getCampaign_offer_uuid() {
        return Optional.ofNullable(campaign_offer_uuid).orElse(NULL_UUID);
    }

    public static RowMapper<CampaignAssociation> getRowMapper() {
        return (rs, rowNum) -> new CampaignAssociation()
                .setCampaign_id(rs.getLong("campaign_id"))
                .setCampaign_uuid(Optional.ofNullable(rs.getString("campaign_uuid")).orElse(NULL_UUID))
                .setCampaign_start_at(Optional.ofNullable(rs.getTimestamp("campaign_start_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_end_at(Optional.ofNullable(rs.getTimestamp("campaign_end_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_enabled(rs.getInt("campaign_enabled"))
                .setCampaign_updated_at(Optional.ofNullable(rs.getTimestamp("campaign_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_deleted_at(Optional.ofNullable(rs.getTimestamp("campaign_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_offer_id(rs.getLong("campaign_offer_id"))
                .setCampaign_offer_uuid(Optional.ofNullable(rs.getString("campaign_offer_uuid")).orElse(NULL_UUID))
                .setCampaign_offer_updated_at(Optional.ofNullable(rs.getTimestamp("campaign_offer_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_offer_deleted_at(Optional.ofNullable(rs.getTimestamp("campaign_offer_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setCampaign_offer_products_count(rs.getLong("campaign_offer_products_count"));
    }

    public boolean shouldCampaignOfferDeleted() {
        return NULL_ID != this.getCampaign_offer_id()
                && (shouldCampaignDeleted() || campaign_deleted_at != null);
    }

    public boolean shouldCampaignDeleted() {
        return NULL_ID != getCampaign_id()
                && (campaign_deleted_at != null
                || campaign_enabled <= 0
                || Optional.ofNullable(campaign_end_at).map(endTime -> endTime.isBefore(LocalDateTime.now())).orElse(false))
                || campaign_offer_products_count > 0;
    }

    public boolean shouldDeleted() {
        return shouldCampaignDeleted()
                || shouldCampaignOfferDeleted();
    }

    public String getAssociationId() {
        return getAssociationId(getCampaign_id(), getCampaign_offer_id());
    }

    public String getAssociationId(Long campaignId, Long campaignOfferId) {
        // C_{campaign_id}_{campaign_offer_id}
        return String.format("C_%d_%d", campaignId, campaignOfferId);
    }

    public List<String> getShouldDeletedIdsForUpdate() {
        return NULL_ID == getCampaign_offer_id() ?  List.of() : List.of(getAssociationId(getCampaign_id(), NULL_ID));
    }

    public List<String> getShouldDeletedIdsForDelete() {
        return getShouldDeletedIdsForUpdate();
    }

    public LocalDateTime getUpdatedAt() {
        return Stream.of(campaign_deleted_at,
                        campaign_updated_at,
                        campaign_offer_deleted_at,
                        campaign_offer_updated_at
                )
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(null);
    }
}
