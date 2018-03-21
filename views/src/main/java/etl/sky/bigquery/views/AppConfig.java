package etl.sky.bigquery.views;

/**
 * @author dmytry.trunykov@zorallabs.com
 */
public class AppConfig {

    /**
     * An identifier to denote this run.
     */
    private final String batchId;

    /**
     * Project ID.
     */
    private final String projectId;

    /**
     * Name of a bucket with DDL files for views.
     */
    private final String bucketName;

    /**
     * How many threads can be used for parallel execution of task(s).
     */
    private final int threadPoolSize;

    public AppConfig(String batchId, String projectId, String bucketName, int threadPoolSize) {
        super();
        this.batchId = batchId;
        this.projectId = projectId;
        this.bucketName = bucketName;
        this.threadPoolSize = threadPoolSize;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getBucketName() {
        return bucketName;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    

}
