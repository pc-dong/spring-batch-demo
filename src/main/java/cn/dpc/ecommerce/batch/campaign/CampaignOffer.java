package cn.dpc.ecommerce.batch.campaign;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.time.LocalDateTime;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;

@Data
@Accessors(chain = true)
public class CampaignOffer {
    private Long campaign_offer_id;
    private String campaign_offer_uuid;
    private String campaign_offer_name;
    private LocalDateTime campaign_offer_deleted_at;
    private LocalDateTime campaign_offer_updated_at;
    private Integer campaign_enabled;
    private LocalDateTime campaign_deleted_at;
    private LocalDateTime campaign_end_at;

    public static RowMapper<CampaignOffer> getRowMapper() {
        return new BeanPropertyRowMapper<>(CampaignOffer.class);
    }

    public boolean shouldCampaignDeleted() {
        return campaign_deleted_at != null
                || campaign_enabled <= 0
                || Optional.ofNullable(campaign_end_at).map(endTime -> endTime.isBefore(LocalDateTime.now())).orElse(false);
    }

    public boolean shouldDeleted() {
        return shouldCampaignDeleted() || campaign_offer_deleted_at != null;
    }

    public LocalDateTime getUpdatedAt() {
        return campaign_offer_updated_at;
    }

}
