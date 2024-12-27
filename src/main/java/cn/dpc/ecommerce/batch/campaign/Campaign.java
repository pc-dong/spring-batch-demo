package cn.dpc.ecommerce.batch.campaign;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.time.LocalDateTime;
import java.util.Optional;

@Data
@Accessors(chain = true)
public class Campaign {
    private Long campaign_id;
    private String campaign_uuid;
    private String campaign_name;
    private String campaign_subtitle;
    private String campaign_tag_names;
    private String campaign_type;
    private String campaign_warm_up_type;
    private Boolean campaign_enabled;
    private LocalDateTime campaign_start_at;
    private LocalDateTime campaign_end_at;
    private LocalDateTime campaign_online_at;
    private String campaign_warm_up_date;
    private String campaign_warm_up;
    private String campaign_description;
    private String campaign_image;
    private String campaign_tags;
    private LocalDateTime campaign_deleted_at;
    private LocalDateTime campaign_updated_at;

    public static RowMapper<Campaign> getRowMapper() {
        return new BeanPropertyRowMapper<>(Campaign.class);
    }

    public boolean shouldDeleted() {
        return campaign_deleted_at != null
                        || !campaign_enabled
                        || Optional.ofNullable(campaign_end_at)
                        .map(end -> end.isBefore(LocalDateTime.now()))
                        .orElse(false);
    }

    public LocalDateTime getUpdatedAt() {
        return campaign_updated_at;
    }

}
