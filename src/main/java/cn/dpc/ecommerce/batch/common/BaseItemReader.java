package cn.dpc.ecommerce.batch.common;

import cn.dpc.ecommerce.batch.consts.Constants;
import cn.dpc.ecommerce.batch.location.Outlet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

@Slf4j
public abstract class BaseItemReader<T> implements ItemReader<T> {
    protected final JdbcPagingItemReader<T> reader;

    public BaseItemReader(JdbcPagingItemReader<T> reader) {
        this.reader = reader;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        LocalDateTime lastUpdateTime = Optional.ofNullable(executionContext.get(Constants.LAST_UPDATE_TIME))
                .map(o -> (LocalDateTime) o).orElse(LocalDateTime.now().minusYears(5));
        LocalDateTime endTime = Optional.ofNullable(executionContext.get(Constants.END_TIME, LocalDateTime.class))
                .orElse(LocalDateTime.now());

        this.reader.setParameterValues(Map.of("startTime", lastUpdateTime, "endTime", endTime));
        this.reader.open(executionContext);
        log.info("Last update time in reader: {}", lastUpdateTime);
    }

    @AfterStep
    public void afterStep(StepExecution stepExecution) {
        this.reader.close();
    }

    @Override
    public T read() throws Exception {
        this.reader.afterPropertiesSet();
        return this.reader.read();
    }
}
