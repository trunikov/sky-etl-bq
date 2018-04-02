package etl.sky.bigquery.merge;

import static etl.sky.bigquery.merge.Main.MDC_KEY_JOBID;

import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Task implements Callable<Void> {

    private final static Logger log = LoggerFactory.getLogger(Task.class);
    
    private final String jobIdTxt;

    private final JobId jobId;

    private final TableId destTblId;

    private final String sql;

    private final CreateDisposition createDisposition;

    private final WriteDisposition writeDisposition;

    private Task(String jobIdTxt, JobId jobId, String sql, String dataset, String table, CreateDisposition createDisposition,
            WriteDisposition writeDisposition) {
        this.jobIdTxt = jobIdTxt;
        this.jobId = jobId;
        this.sql = sql;
        this.destTblId = TableId.of(dataset, table);
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        log.info("Created a task {}", toString());
    }

    public static Task of(String batchId, TaskConfig tc) {
        // Create a job ID so that we can safely retry.
        String jobIdTxt = UUID.randomUUID().toString();
        JobId jobId = JobId.of(jobIdTxt);
        String pttrnSql = tc.getSqlPttrn();
        String sql = Utils.preparePttrnSql(pttrnSql, batchId);
        CreateDisposition createDisposition = CreateDisposition.valueOf(tc.getCreateDisposition());
        WriteDisposition writeDisposition = WriteDisposition.valueOf(tc.getWriteDisposition());
        return new Task(jobIdTxt, jobId, sql, tc.getDataSet(), tc.getTableName(), createDisposition, writeDisposition);
    }

    @Override
    public Void call() throws Exception {
        boolean failed = false;
        MDC.put(MDC_KEY_JOBID, StringUtils.abbreviate(jobIdTxt, 16));
        log.info("The task is started.");
        try {
            //@formatter:off
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
                .setUseLegacySql(false)
                .setAllowLargeResults(true)
                .setDestinationTable(destTblId)
                .setCreateDisposition(createDisposition)
                .setWriteDisposition(writeDisposition)
                .build();
            //@formatter:on
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            Job queryJob = null;
            try {
                queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            } catch (BigQueryException e) {
                failed = true;
                log.error("Failed to create a job.", e);
                return null;
            }
            if (queryJob == null) {
                failed = true;
                log.error("Job no longer exists");
                return null;
            } else if (queryJob.getStatus().getError() != null) {
                failed = true;
                // You can also look at queryJob.getStatus().getExecutionErrors() for all
                // errors, not just the latest one.
                String errMsg = queryJob.getStatus().getError().toString();
                log.error("The job failed: {}", errMsg);
                return null;
            }
            return null;
        } catch(Exception e) {
            failed = true;
            log.error("Unexpected failure of the task: " + toString(), e);
            return null;
        } finally {
            if (failed) {
                log.info("The task finished by failure.");
            } else {
                log.info("The task completed as succeded.");
            }
        }
    }

    @Override
    public String toString() {
        return "Task [jobId=" + jobId + ", createDisposition=" + createDisposition + ", writeDisposition="
                + writeDisposition + ", destTblId=" + destTblId + ", sql=" + sql + "]";
    }

}
