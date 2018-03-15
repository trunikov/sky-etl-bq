package etl.sky.bigquery.merge;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Main {

    private final static String CONFIG_CHARSET = "UTF-8";

    public static void main(String[] args) throws Exception {
        CommandLine commandLine = parseCommandLine(args);
        if (commandLine != null) {
            String batchId = commandLine.getOptionValue('b');
            String configFileUrl = commandLine.getOptionValue('c');
            loadConfig(configFileUrl);
            // doJob(configFileUrl, batchId);
        }
    }

    private static CommandLine parseCommandLine(String[] arguments) throws ParseException {
        CommandLineParser cmdLineParser = new DefaultParser();
        Options options = new Options();
        try {
            options.addOption("c", "configFile", true, "URL to a configuration file.");
            options.addOption("b", "batchId", true, "batch ID.");
            CommandLine retVal = cmdLineParser.parse(options, arguments);
            if (!retVal.hasOption("c")) {
                throw new ParseException("Missed mandatory comman line argument -c (--configFile).");
            }
            if (!retVal.hasOption("b")) {
                throw new ParseException("Missed mandatory comman line argument -b (--batchId).");
            }
            return retVal;
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("mvn compile exec:java -Dexec.mainClass=etl.sky.bigquery.merge.Main "
                    + "-Dexec.args=\"<ARGS>\", where ARGS are:", options);
            return null;
        }
    }

    private static void loadConfig(String url) throws IOException, StorageException, URISyntaxException {
        BlobId configBlobId = Utils.fromUrl(url);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        byte[] configBytes = storage.readAllBytes(configBlobId);
        String configText = new String(configBytes, CONFIG_CHARSET);
        System.out.printf("configText: %s", configText);
    }

    private static void doJob(String configFileUrl, String batchId) throws JobException, InterruptedException {
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
    }

}
