package cn.dpc.ecommerce.batch.location;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.time.LocalDateTime;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.activePropertyStatus;

@Data
@Accessors(chain = true)
public class Property {
    private Long property_id;
    private String property_uuid;
    private String pr_name_chinese;
    private String pr_status;
    private String pr_brand_id;
    private String pr_full_brand_code;
    private String pr_sub_brand;
    private String pr_brand_code;
    private String pr_brand_name_chinese;
    private String pr_brand_category_name_english;
    private String pr_brand_category_name_chinese;
    private Integer pr_brand_category_sorting;
    private Integer pr_brand_sorting;
    private String pr_brand_link_color;
    private String pr_brand_primary_color;
    private String pr_brand_logo_url;
    private String pr_brand_name_english;
    private Double pr_distance;
    private Double pr_longitude;
    private Double pr_latitude;
    private String pr_city_code;
    private String pr_city_title_chinese;
    private String pr_city_title_english;
    private String pr_city_title_pinyin;
    private String pr_marsha_code;
    private String pr_address_chinese;
    private String pr_address_english;
    private String pr_name_english;
    private String pr_province_name;
    private String pr_city_name;
    private String pr_district_name;
    private String pr_details_chinese;
    private String pr_details_english;
    private String pr_header_image_url;
    private Integer pr_is_reservation_enabled;
    private Integer pr_is_cp_enabled = 0;
    private LocalDateTime pr_deleted_at;
    private LocalDateTime pr_updated_at;

    public static RowMapper<Property> getRowMapper() {
        return new BeanPropertyRowMapper<>(Property.class);
    }

    public Integer getPr_is_cp_enabled() {
        return Optional.ofNullable(pr_is_cp_enabled).orElse(0);
    }

    public String getFullBrandCode() {
        return pr_brand_code
                + (pr_sub_brand != null && !pr_sub_brand.isEmpty() ? "_" : "")
                + (pr_sub_brand != null ? pr_sub_brand : "");
    }

    public boolean shouldDeleted() {
        return pr_deleted_at != null
                        || !activePropertyStatus.contains(pr_status);
    }

    public LocalDateTime getUpdatedAt() {
        return pr_updated_at;
    }

}
