package etl.sky.bigquery.views;

import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.cloud.storage.StorageException;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Main {

    private final static Logger log = LoggerFactory.getLogger(Main.class);

    private final static String MDC_KEY_BATCHID = "batchId";
    final static String MDC_KEY_JOBID = "jobId";

    private final static String OPT_BATCHID = "b";
    private final static String OPT_BATCHID_LONG = "batchId";
    private final static String OPT_PROJECTID = "p";
    private final static String OPT_PROJECTID_LONG = "projectId";
    private final static String OPT_BACKET = "n";
    private final static String OPT_BACKET_LONG = "backet";
    private final static String OPT_THREADPOOLSIZE = "t";
    private final static String OPT_THREADPOOLSIZE_LONG = "threadPoolSize";

    private final static int THREADPOOL_SIZE_DEF = 5;
    private final static int THREADPOOL_SIZE_MIN = 1;
    private final static int THREADPOOL_SIZE_MAX = 512;

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
            } catch (StorageException e) {
                String errMsg = String.format("Reading from the backet failed: %s", e.getMessage());
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

    private static AppConfig parseCommandLine(String[] arguments) throws ParseException, URISyntaxException {
        CommandLineParser cmdLineParser = new DefaultParser();
        Options options = new Options();
        try {
            options.addOption(OPT_BATCHID, OPT_BATCHID_LONG, true, "batch ID.");
            options.addOption(OPT_PROJECTID, OPT_PROJECTID_LONG, true, "project ID.");
            options.addOption(OPT_BACKET, OPT_BACKET_LONG, true, "backet name.");
            options.addOption(OPT_THREADPOOLSIZE, OPT_THREADPOOLSIZE_LONG, true,
                    "pool size of threads to process task(s). By default it is " + THREADPOOL_SIZE_DEF + ".");
            CommandLine cmdLine = cmdLineParser.parse(options, arguments);
            if (!cmdLine.hasOption(OPT_BATCHID)) {
                throw new ParseException(
                        "Missed mandatory comman line argument -" + OPT_BATCHID + " (--" + OPT_BATCHID_LONG + ").");
            }
            if (!cmdLine.hasOption(OPT_PROJECTID)) {
                throw new ParseException(
                        "Missed mandatory comman line argument -" + OPT_PROJECTID + " (--" + OPT_PROJECTID_LONG + ").");
            }
            if (!cmdLine.hasOption(OPT_BACKET)) {
                throw new ParseException(
                        "Missed mandatory comman line argument -" + OPT_BACKET + " (--" + OPT_BACKET_LONG + ").");
            }
            String optT = cmdLine.getOptionValue(OPT_THREADPOOLSIZE);
            int threadPoolSize = THREADPOOL_SIZE_DEF;
            try {
                threadPoolSize = Integer.valueOf(optT);
            } catch (NumberFormatException e) {
                throw new ParseException("The string can't be parsed to int: " + optT);
            }
            if (threadPoolSize < THREADPOOL_SIZE_MIN || threadPoolSize > THREADPOOL_SIZE_MAX) {
                throw new ParseException("Invalid value of a thread pool size " + threadPoolSize
                        + ". Expected an integer in a range " + THREADPOOL_SIZE_MIN + ".." + THREADPOOL_SIZE_MAX + ".");
            }
            String gsUrl = cmdLine.getOptionValue(OPT_BACKET);
            Pair<String, String> parsedUrl = Utils.fromUrl(gsUrl);
            String backetName = parsedUrl.getLeft();
            String folderName = parsedUrl.getRight();
            return new AppConfig(cmdLine.getOptionValue(OPT_BATCHID), cmdLine.getOptionValue(OPT_PROJECTID), backetName,
                    folderName, threadPoolSize);
        } catch (ParseException e) {
            log.error("Invalid command line: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar view2table.jar <ARGS>", options);
            return null;
        }
    }

}
