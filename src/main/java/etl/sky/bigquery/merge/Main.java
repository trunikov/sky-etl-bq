package etl.sky.bigquery.merge;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static void main(String[] args) throws Exception {
        CommandLine commandLine = parseCommandLine(args);
        if (commandLine != null) {
            String batchId = commandLine.getOptionValue('b');
            String configFileUrl = commandLine.getOptionValue('c');
            //loadConfig(configFileUrl);
            //@formatter:off
            List<TaskConfig> tasksConfigs = Arrays.asList(
                new TaskConfig(),
                new TaskConfig()
            );
            //@formatter:on
            executeTasks(tasksConfigs);
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

    private static List<TaskConfig> loadConfig(String url) throws IOException, StorageException, URISyntaxException {
        BlobId configBlobId = Utils.fromUrl(url);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        byte[] configBytes = storage.readAllBytes(configBlobId);
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<TaskConfig>> tref = new TypeReference<List<TaskConfig>>() {};
        List<TaskConfig> tasksConfigs = (List<TaskConfig>) mapper.readValue(configBytes, tref);
System.out.println(tasksConfigs);
        return tasksConfigs;
    }

    private static void executeTasks(List<TaskConfig> tasksConfigs) throws JobException, InterruptedException {
        List<Task> tasks = tasksConfigs.stream().map(tc -> {
            return new Task();
        }).collect(Collectors.toList());
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        executorService.invokeAll(tasks);
        executorService.shutdown();
    }

}
