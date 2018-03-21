package etl.sky.bigquery.merge;

import java.io.IOException;
import java.net.URISyntaxException;

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

import com.google.cloud.storage.StorageException;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Main {

    private final static Logger log = LoggerFactory.getLogger(Main.class);

    public final static String MDC_KEY_BATCHID = "batchId";
    public final static String MDC_KEY_JOBID = "jobId";

    private final static String OPT_CONFIGFILE = "c";
    private final static String OPT_BATCHID = "b";
    private final static String OPT_THREADPOOLSIZEID = "t";

    private final static int DEF_THREADPOOL_SIZE = 5;

    public static void main(String[] args) {
        try {
            AppConfig opts = parseCommandLine(args);
            if (opts == null) {
                System.exit(1);
            }
            MDC.put(MDC_KEY_BATCHID, StringUtils.abbreviate(opts.getBatchId(), 16));
            try {
                App app = new App(opts);
                app.call();
            } catch (URISyntaxException e) {
                String errMsg = "Invalid URL to the configuration file: " + opts.getUrlConfig();
                if (log.isDebugEnabled()) {
                    log.error(errMsg, e);
                } else {
                    log.error(errMsg);
                }
                System.exit(1);
            } catch (StorageException e) {
                String errMsg = String.format("Reading of the configuration file (%s) failed: %s", opts.getUrlConfig(),
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
        } catch (Exception e) {
            log.error("Unexpected failure.", e);
            System.exit(1);
        }
    }

    private static AppConfig parseCommandLine(String[] arguments) throws ParseException {
        CommandLineParser cmdLineParser = new DefaultParser();
        Options options = new Options();
        try {
            options.addOption(OPT_CONFIGFILE, "configFile", true, "URL to a configuration file.");
            options.addOption(OPT_BATCHID, "batchId", true, "batch ID.");
            options.addOption(OPT_THREADPOOLSIZEID, "threadPoolSize", true,
                    "pool size of threads to process task(s). By default it is " + DEF_THREADPOOL_SIZE + ".");
            CommandLine cmdLine = cmdLineParser.parse(options, arguments);
            if (!cmdLine.hasOption(OPT_CONFIGFILE)) {
                throw new ParseException("Missed mandatory comman line argument -c (--configFile).");
            }
            if (!cmdLine.hasOption(OPT_BATCHID)) {
                throw new ParseException("Missed mandatory comman line argument -b (--batchId).");
            }
            String optT = cmdLine.getOptionValue(OPT_THREADPOOLSIZEID);
            int threadPoolSize = DEF_THREADPOOL_SIZE;
            try {
                threadPoolSize = Integer.valueOf(optT);
            } catch (NumberFormatException e) {
                throw new ParseException("The string can't be parsed to int: " + optT);
            }
            if (threadPoolSize < 1 || threadPoolSize > 512) {
                throw new ParseException("Invalid value of a thread pool size " + threadPoolSize
                        + ". Expected an integer in a range 1..512");
            }
            return new AppConfig(cmdLine.getOptionValue('b'), cmdLine.getOptionValue('c'), threadPoolSize);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar view2table.jar <ARGS>", options);
            return null;
        }
    }

}
