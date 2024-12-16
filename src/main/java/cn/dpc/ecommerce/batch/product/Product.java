package cn.dpc.ecommerce.batch.product;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
public class Product {
    private String id;
    private String uuid;
    private String title;
    private String status;
    private String groupId;
    private Integer groupProductRank;
    private LocalDateTime updatedAt;
}
