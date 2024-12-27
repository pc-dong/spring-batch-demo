package cn.dpc.ecommerce.batch.location;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.activePropertyStatus;

@Data
@Accessors(chain = true)
public class Outlet {
    private Long outlet_id;
    private String outlet_uuid;
    private String outlet_name_chinese;
    private String outlet_type;
    private String property_marsha_code;
    private String outlet_status;
    private String outlet_service;
    private Integer outlet_rank;
    private String outlet_name_english;
    private List<String> outlet_operating_hours;
    private String outlet_arrangement_chinese;
    private String outlet_arrangement_english;
    private String outlet_details_chinese;
    private String outlet_details_english;
    private String outlet_media_type;
    private String outlet_video_cover_image_url;
    private String outlet_cover_image_url;
    private List<String> outlet_banner_image_urls;
    private String outlet_address;
    private String outlet_short_desc_chinese;
    private String outlet_mobile_phone;
    private String outlet_mobile_country_code;
    private String outlet_landline;
    private String outlet_region_code;
    private String outlet_landline_country_code;
    private String outlet_landline_extension;
    private Integer outlet_online_booking_flag;
    private LocalDateTime outlet_deleted_at;
    private LocalDateTime outlet_updated_at;

    private Long property_id;
    private String property_status;
    private String property_deleted_at;
    private Integer property_is_cp_enabled = 0;

    public Integer getProperty_is_cp_enabled() {
        return Optional.ofNullable(property_is_cp_enabled).orElse(0);
    }

    public static RowMapper<Outlet> getRowMapper() {
        return new BeanPropertyRowMapper<>(Outlet.class);
    }



    public boolean shouldDeleted() {
        return (outlet_deleted_at != null || "INVISIBLE".equals(outlet_status))
                || (property_deleted_at != null || !activePropertyStatus.contains(property_status));
    }

    public LocalDateTime getUpdatedAt() {
        return outlet_updated_at;
    }
}
