package cn.dpc.ecommerce.batch.time;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static cn.dpc.ecommerce.batch.consts.Constants.APP_NAME;
import static cn.dpc.ecommerce.batch.consts.Constants.TYPE;
import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

@Slf4j
public class LastUpdateTimeSaveStep implements Step {

    private final JdbcTemplate jdbcTemplate;

    public LastUpdateTimeSaveStep(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public String getName() {
        return "LastUpdateTimeSaveStep";
    }


    @Override
    public void execute(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        var type = executionContext.getString(TYPE);
        var appName = executionContext.getString(APP_NAME);
        log.info("updatedLastUpdateTime: {}", stepExecution.getJobExecution().getExecutionContext().get(UPDATED_UPDATE_TIME));
        var querySql = "select count(1) from update_time where name = ? and app_name = ?";
        LocalDateTime lastUpdateTime = Optional.ofNullable(executionContext.get(UPDATED_UPDATE_TIME))
                .map(o -> (LocalDateTime) o).orElse(LocalDateTime.now().minusYears(5));

        List<Long> counts = jdbcTemplate.query(querySql, new Object[]{type, appName}, new int[]{Types.VARCHAR, Types.VARCHAR},
                (rs, rowNum) -> rs.getLong(1));
        if (!CollectionUtils.isEmpty(counts) && counts.get(0) > 0) {
            String updateSql = "update update_time set last_update_time = ? where name = ? and app_name = ?";
            jdbcTemplate.update(updateSql, Timestamp.valueOf(lastUpdateTime), type, appName);
        } else {
            String insertSql = "insert into update_time (name, app_name, last_update_time) values (?, ?, ?)";
            jdbcTemplate.update(insertSql, type, appName, Timestamp.valueOf(lastUpdateTime));
        }

        stepExecution.setStatus(BatchStatus.COMPLETED);
        stepExecution.setExitStatus(ExitStatus.COMPLETED);
    }
}
