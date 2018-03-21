package etl.sky.bigquery.merge;

/**
 * A container to hold values of command line options.
 * 
 * @author dmytro.trunykov@zorallabs.com
 */
public class AppConfig {

    /**
     * An identifier to denote this run.
     */
    private final String batchId;

    /**
     * An URL in a form 'gs://<backet>/<file>' to a configuration file with tasks for an execution.
     */
    private final String urlConfig;

    /**
     * How many threads can be used for parallel execution of task(s).
     */
    private final int threadPoolSize;

    public AppConfig(String batchId, String urlConfig, int threadPoolSize) {
        this.batchId = batchId;
        this.urlConfig = urlConfig;
        this.threadPoolSize = threadPoolSize;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getUrlConfig() {
        return urlConfig;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

}
