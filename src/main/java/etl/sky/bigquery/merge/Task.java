package etl.sky.bigquery.merge;

import java.util.UUID;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Task implements Callable<Void> {

    private final static Logger log = LoggerFactory.getLogger(Task.class);

    @Override
    public Void call() throws Exception {
        log.info("Task " + this + " started.");
        Thread.sleep(5000 + (long) (Math.random() * 10000L));
        log.info("Task " + this + " finished.");
        
        /*
        QueryJobConfiguration queryConfig = QueryJobConfiguration
                .newBuilder("SELECT * FROM " + "FROM `skyuk-uk-nowtv-bit-ecc-dev.test.transactions_2017` ")
                // Use standard SQL syntax for queries.
                // See: https://cloud.google.com/bigquery/sql-reference/
                .setUseLegacySql(false).build();
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        QueryResponse response = bigquery.getQueryResults(jobId);

        TableResult result = queryJob.getQueryResults();

        // Print all pages of the results.
        for (FieldValueList row : result.iterateAll()) {
            String url = row.get("url").getStringValue();
            long viewCount = row.get("view_count").getLongValue();
            System.out.printf("url: %s views: %d%n", url, viewCount);
        }
        */
        return null;
    }

}
