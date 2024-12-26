package cn.dpc.ecommerce.batch.product;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static cn.dpc.ecommerce.batch.consts.Constants.NULL_ID;

@Data
@Accessors(chain = true)
public class Product {
    private Long product_id;
    private String product_uuid;
    private String product_type;
    private String product_subtype;
    private String product_title;
    private String product_status;
    private String product_warm_up_type;
    private Boolean product_has_online_flag;
    private String product_dining_periods;
    private BigDecimal product_sale_price;
    private BigDecimal product_markdown_price;
    private String product_invisible;
    private LocalDateTime product_preheat_date;
    private LocalDateTime product_sale_start_at;
    private LocalDateTime product_sale_stop_at;
    private String product_cuisine_id;
    private String product_cuisine_name_cn;
    private String product_media_type;
    private String product_cover_image_url;
    private String product_video_cover_image_url;
    private String product_banner_cover_image_url;
    private BigDecimal product_value;
    private Long product_sold_qty;
    private Long product_inventory;
    private Boolean product_member_only;
    private String product_current_version_uuid;
    private LocalDateTime configurable_product_deleted_at;
    private LocalDateTime configurable_product_updated_at;
    private Long configurable_product_id;
    private Long product_group_id;
    private String product_group_status;
    private Integer product_group_rank;
    private String product_group_name;
    private LocalDateTime product_group_deleted_at;
    private LocalDateTime product_group_updated_at;
    private LocalDateTime product_created_at;
    private LocalDateTime product_deleted_at;
    private LocalDateTime product_updated_at;
    private String product_tag_names;
    private String product_tags;

    public List<String> getProductDiningPeriods() {
        return Optional.ofNullable(product_dining_periods)
                .map(s -> List.of(s.split(",")))
                .orElse(List.of());
    }

    private boolean isGroupDeleted() {
        return product_group_deleted_at != null || !"PUBLISHED".equals(product_group_status);
    }

    public Long getProduct_group_id() {
        return isGroupDeleted() ? NULL_ID : product_group_id;
    }

    public String getProduct_group_status() {
        return isGroupDeleted() ? null : product_group_status;
    }

    public Integer getProduct_group_rank() {
        return isGroupDeleted() ? 0 : product_group_rank;
    }

    public String getProduct_group_name() {
        return isGroupDeleted() ? null : product_group_name;
    }

    public Long getDistinct_key_param() {
        if (NULL_ID != this.getProduct_group_id()) {
            return this.getProduct_group_id();
        }

        return this.getProduct_id();
    }

    public static RowMapper<Product> getRowMapper() {
        return (rs, rowNum) -> new Product()
                .setProduct_id(rs.getLong("product_id"))
                .setProduct_uuid(rs.getString("product_uuid"))
                .setProduct_type(rs.getString("product_type"))
                .setProduct_subtype(rs.getString("product_subtype"))
                .setProduct_title(rs.getString("product_title"))
                .setProduct_status(rs.getString("product_status"))
                .setProduct_warm_up_type(rs.getString("product_warm_up_type"))
                .setProduct_has_online_flag(rs.getBoolean("product_has_online_flag"))
                .setProduct_dining_periods(rs.getString("product_dining_periods"))
                .setProduct_sale_price(rs.getBigDecimal("product_sale_price"))
                .setProduct_markdown_price(rs.getBigDecimal("product_markdown_price"))
                .setProduct_invisible(rs.getString("product_invisible"))
                .setProduct_preheat_date(Optional.ofNullable(rs.getTimestamp("product_preheat_date")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_sale_start_at(Optional.ofNullable(rs.getTimestamp("product_sale_start_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_sale_stop_at(Optional.ofNullable(rs.getTimestamp("product_sale_stop_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_cuisine_id(rs.getString("product_cuisine_id"))
                .setProduct_media_type(rs.getString("product_media_type"))
                .setProduct_cover_image_url(rs.getString("product_cover_image_url"))
                .setProduct_video_cover_image_url(rs.getString("product_video_cover_image_url"))
                .setProduct_banner_cover_image_url(rs.getString("product_banner_cover_image_url"))
                .setProduct_value(rs.getBigDecimal("product_value"))
                .setProduct_sold_qty(rs.getLong("product_sold_qty"))
                .setProduct_inventory(rs.getLong("product_inventory"))
                .setProduct_member_only(rs.getBoolean("product_member_only"))
                .setProduct_current_version_uuid(rs.getString("product_current_version_uuid"))
                .setConfigurable_product_deleted_at(Optional.ofNullable(rs.getTimestamp("configurable_product_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setConfigurable_product_updated_at(Optional.ofNullable(rs.getTimestamp("configurable_product_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setConfigurable_product_id(rs.getLong("configurable_product_id"))
                .setProduct_group_id(rs.getLong("product_group_id"))
                .setProduct_group_status(rs.getString("product_group_status"))
                .setProduct_group_rank(rs.getInt("product_group_rank"))
                .setProduct_group_name(rs.getString("product_group_name"))
                .setProduct_group_deleted_at(Optional.ofNullable(rs.getTimestamp("product_group_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_group_updated_at(Optional.ofNullable(rs.getTimestamp("product_group_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_created_at(rs.getTimestamp("product_created_at") != null ? rs.getTimestamp("product_created_at").toLocalDateTime() : null)
                .setProduct_deleted_at(Optional.ofNullable(rs.getTimestamp("product_deleted_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_updated_at(Optional.ofNullable(rs.getTimestamp("product_updated_at")).map(Timestamp::toLocalDateTime).orElse(null))
                .setProduct_tag_names(rs.getString("product_tag_names"))
                .setProduct_tags(rs.getString("product_tags"));

    }

    public boolean shouldDeleted() {
        return product_deleted_at != null || !"ONLINE".equals(product_status) || product_inventory <= 0;
    }

    public LocalDateTime getUpdatedAt() {
        return Stream.of(product_updated_at,
                        product_deleted_at,
                        configurable_product_updated_at,
                        configurable_product_deleted_at,
                        product_group_updated_at,
                        product_group_deleted_at
                )
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(null);
    }
}
