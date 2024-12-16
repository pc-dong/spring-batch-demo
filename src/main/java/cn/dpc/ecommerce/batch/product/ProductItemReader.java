package cn.dpc.ecommerce.batch.product;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ProductItemReader implements ItemReader<Product> {
    private final JdbcPagingItemReader<Product> reader;

    public ProductItemReader(DataSource dataSource, int pageSize, int fetchSize) {
        this.reader = new JdbcPagingItemReaderBuilder<Product>()
                .name("masterItemReader")
                .dataSource(dataSource)
                .rowMapper((rs, rowNum) -> new Product()
                        .setTitle(rs.getString("title"))
                        .setId(rs.getString("id"))
                        .setUuid(rs.getString("uuid"))
                        .setStatus(rs.getString("status"))
                        .setGroupId(rs.getString("group_id"))
                        .setGroupProductRank(rs.getInt("rank"))
                        .setUpdatedAt(rs.getTimestamp("updated_at").toLocalDateTime())
                )
                .selectClause("p.id, p.uuid, p.title, p.status, cp.group_id, cp.rank, cpg.status as group_status, p.updated_at")
                .fromClause("""
                    products p
                    LEFT JOIN configurable_product cp on p.id = cp.product_id and p.deleted_at is null
                    and cp.deleted_at is null
                    LEFT JOIN configurable_product_group cpg on cp.group_id = cpg.id and cpg.deleted_at is null
                    """)
                .whereClause("p.updated_at > :updatedAt")
                .sortKeys(Map.of("p.id", Order.ASCENDING))
                .pageSize(pageSize)
                .fetchSize(fetchSize)
                .build();

    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        LocalDateTime lastUpdateTime = Optional.ofNullable(stepExecution.getJobExecution().getExecutionContext().get("lastUpdateTime"))
                .map(o -> (LocalDateTime) o).orElse(LocalDateTime.now().minusYears(5));
        this.reader.setParameterValues(Map.of("updatedAt", lastUpdateTime));
        log.info("Last update time in reader: {}", lastUpdateTime);
    }

    @Override
    public Product read() throws Exception {
        this.reader.afterPropertiesSet();
        return this.reader.read();
    }
}
