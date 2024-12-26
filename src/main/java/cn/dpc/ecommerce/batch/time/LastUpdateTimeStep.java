package cn.dpc.ecommerce.batch.time;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;

import static cn.dpc.ecommerce.batch.consts.Constants.APP_NAME;
import static cn.dpc.ecommerce.batch.consts.Constants.END_TIME;
import static cn.dpc.ecommerce.batch.consts.Constants.LAST_UPDATE_TIME;
import static cn.dpc.ecommerce.batch.consts.Constants.TYPE;
import static cn.dpc.ecommerce.batch.consts.Constants.UPDATED_UPDATE_TIME;

@Slf4j
public class LastUpdateTimeStep implements Step {
    private final JdbcTemplate jdbcTemplate;
    private final String appName;
    private final String type;

    public LastUpdateTimeStep(DataSource dataSource, String appName, String type) {
        jdbcTemplate = new JdbcTemplate(dataSource);
        this.appName = appName;
        this.type = type;
    }


    @Override
    public String getName() {
        return "LastUpdateTimeStep";
    }


    @Override
    public void execute(StepExecution stepExecution) {
        var jobParameters = stepExecution.getJobParameters();
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        var appName = jobParameters.getString(APP_NAME);
        appName = StringUtils.hasLength(appName) ? appName : this.appName;
        var lastUpdateTime = jobParameters.getLocalDateTime(LAST_UPDATE_TIME);
        if (null == lastUpdateTime) {
            String querySql = "select last_update_time from update_time where name = ? AND app_name = ?";
            List<LocalDateTime> lastUpdateTimes = jdbcTemplate.query(querySql, new Object[]{this.type, appName},
                    new int[]{Types.VARCHAR, Types.VARCHAR},
                    (rs, rowNum) -> rs.getTimestamp("last_update_time").toLocalDateTime());

            if (!lastUpdateTimes.isEmpty()) {
                executionContext.put(LAST_UPDATE_TIME, lastUpdateTimes.get(0));
            } else {
                executionContext.put(LAST_UPDATE_TIME, LocalDateTime.now().minusYears(5));
            }
        } else {
            executionContext.put(LAST_UPDATE_TIME, lastUpdateTime);
        }

        executionContext.put(UPDATED_UPDATE_TIME, executionContext.get(LAST_UPDATE_TIME));
        executionContext.put(TYPE, this.type);
        executionContext.put(END_TIME, LocalDateTime.now());
        executionContext.put(APP_NAME, appName);

        log.info("lastUpdateTime: {}", executionContext.get(LAST_UPDATE_TIME));
        log.info("endTime: {}", executionContext.get("endTime"));

        stepExecution.setStatus(BatchStatus.COMPLETED);
        stepExecution.setExitStatus(ExitStatus.COMPLETED);
    }
}
