package etl.sky.bigquery.merge;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Main {

    private final static Logger log = LoggerFactory.getLogger(Main.class);

    public final static String MDC_KEY_BATCHID = "batchId";
    public final static String MDC_KEY_JOBID = "jobId";

    public static void main(String[] args) {
        try {
            CommandLine commandLine = parseCommandLine(args);
            if (commandLine != null) {
                String batchId = commandLine.getOptionValue('b');
                String configFileUrl = commandLine.getOptionValue('c');
                MDC.put(MDC_KEY_BATCHID, StringUtils.abbreviate(batchId, 16));
                try {
                    List<TaskConfig> tasksConfigs = loadConfig(configFileUrl);
                    executeTasks(batchId, tasksConfigs);
                } catch (URISyntaxException e) {
                    String errMsg = "Invalid URL to the configuration file: " + configFileUrl;
                    if (log.isDebugEnabled()) {
                        log.error(errMsg, e);
                    } else {
                        log.error(errMsg);
                    }
                    System.exit(1);
                } catch (StorageException e) {
                    String errMsg = String.format("Reading of the configuration file (%s) failed: %s", configFileUrl,
                            e.getMessage());
                    if (log.isDebugEnabled()) {
                        log.error(errMsg, e);
                    } else {
                        log.error(errMsg);
                    }
                    System.exit(1);
                } catch (IOException e) {
                    String errMsg = "Failed to parse the configuration file: " + e.getMessage();
                    if (log.isDebugEnabled()) {
                        log.error(errMsg, e);
                    } else {
                        log.error(errMsg);
                    }
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            log.error("Unexpected failure.", e);
            System.exit(1);
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
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("mvn compile exec:java -Dexec.mainClass=etl.sky.bigquery.merge.Main "
                    + "-Dexec.args=\"<ARGS>\", where ARGS are:", options);
            return null;
        }
    }

    private static List<TaskConfig> loadConfig(String url) throws IOException, StorageException, URISyntaxException {
        BlobId configBlobId = Utils.fromUrl(url);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        byte[] configBytes = storage.readAllBytes(configBlobId);
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<TaskConfig>> tref = new TypeReference<List<TaskConfig>>() {
        };
        @SuppressWarnings("unchecked")
        List<TaskConfig> tasksConfigs = (List<TaskConfig>) mapper.readValue(configBytes, tref);
        return tasksConfigs;
    }

    private static void executeTasks(String batchId, List<TaskConfig> tasksConfigs)
            throws JobException, InterruptedException {
        List<Task> tasks = tasksConfigs.stream().map(tc -> {
            return Task.of(batchId, tc);
        }).collect(Collectors.toList());
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        executorService.invokeAll(tasks);
        executorService.shutdown();
    }

}
