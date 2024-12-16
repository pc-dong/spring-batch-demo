package cn.dpc.ecommerce.batch.time;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.List;

public class LastUpdateTimeStep implements Step {

    private final JdbcTemplate jdbcTemplate;
    private final String type;

    public LastUpdateTimeStep(DataSource dataSource, String type) {
        jdbcTemplate = new JdbcTemplate(dataSource);
        this.type = type;
    }


    @Override
    public String getName() {
        return "LastUpdateTimeStep";
    }



    @Override
    public void execute(StepExecution stepExecution) {
        List<LocalDateTime> lastUpdateTime = jdbcTemplate.query("select last_update_time from update_time where name = '" +this.type+ "'", (rs, rowNum) -> rs.getTimestamp("last_update_time").toLocalDateTime());

        if (!lastUpdateTime.isEmpty()) {
            stepExecution.getJobExecution().getExecutionContext().put("lastUpdateTime", lastUpdateTime.get(0));
        } else {
            stepExecution.getJobExecution().getExecutionContext().put("lastUpdateTime", LocalDateTime.now().minusYears(5));
        }

        stepExecution.setStatus(BatchStatus.COMPLETED);
        stepExecution.setExitStatus(ExitStatus.COMPLETED);
    }
}
