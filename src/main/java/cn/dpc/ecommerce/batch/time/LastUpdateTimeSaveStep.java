package cn.dpc.ecommerce.batch.time;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class LastUpdateTimeSaveStep implements Step {

    private final JdbcTemplate jdbcTemplate;
    private final String type;

    public LastUpdateTimeSaveStep(DataSource dataSource, String type) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.type = type;
    }


    @Override
    public String getName() {
        return "LastUpdateTimeSaveStep";
    }



    @Override
    public void execute(StepExecution stepExecution) {
        System.out.println(stepExecution.getJobExecution().getExecutionContext().get("lastUpdateTime"));
        String querySql = "select count(1) from update_time where name = '" + this.type + "'";
        LocalDateTime lastUpdateTime = Optional.ofNullable(stepExecution.getJobExecution().getExecutionContext().get("lastUpdateTime"))
                .map(o -> (LocalDateTime) o).orElse(LocalDateTime.now().minusYears(5));

        List<Long> counts = jdbcTemplate.query(querySql, (rs, rowNum) -> rs.getLong(1));
        if (!CollectionUtils.isEmpty(counts) && counts.get(0) > 0) {
            String updateSql = "update update_time set last_update_time = ? where name = ?";
            jdbcTemplate.update(updateSql, Timestamp.valueOf(lastUpdateTime), this.type);
        } else {
            String insertSql = "insert into update_time (name, last_update_time) values (?, ?)";
            jdbcTemplate.update(insertSql, this.type, Timestamp.valueOf(lastUpdateTime));
        }

        stepExecution.setStatus(BatchStatus.COMPLETED);
        stepExecution.setExitStatus(ExitStatus.COMPLETED);
    }
}
