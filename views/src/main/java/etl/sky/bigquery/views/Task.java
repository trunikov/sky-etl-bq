package etl.sky.bigquery.views;

import static etl.sky.bigquery.views.Main.MDC_KEY_JOBID;

import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Task implements Callable<Void> {

    private final static Logger log = LoggerFactory.getLogger(Task.class);

    private final TaskFailureNotifier taskFailureNotifier;

    private final String datasetName;

    private final String viewName;

    private final String viewQuery;

    private Task(TaskFailureNotifier taskFailureNotifier, String datasetName, String viewName, String viewQuery) {
        this.taskFailureNotifier = taskFailureNotifier;
        this.datasetName = datasetName;
        this.viewName = viewName;
        this.viewQuery = viewQuery;
    }

    public static Task of(TaskFailureNotifier taskFailureNotifier, String datasetName, String viewName,
            String viewQuery) {
        return new Task(taskFailureNotifier, datasetName, viewName, viewQuery);
    }

    @Override
    public Void call() throws Exception {
        boolean failed = false;
        String threadLabel = UUID.randomUUID().toString();
        MDC.put(MDC_KEY_JOBID, StringUtils.abbreviate(threadLabel, 16));
        log.info("The task is started.");
        try {
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            TableId viewId = TableId.of(datasetName, viewName);
            boolean deleted = bigQuery.delete(viewId);
            if (deleted) {
                log.info("Old definition of the view '{}' has been deleted.", viewId);
            }
            TableDefinition viewDefinition = ViewDefinition.of(viewQuery);
            TableInfo viewInfo = TableInfo.of(viewId, viewDefinition);
            bigQuery.create(viewInfo);
            log.info("The view '{}' has been successfully created.", viewId);
            return null;
        } catch (Exception e) {
            failed = true;
            log.error("Unexpected failure in the task: " + toString(), e);
            return null;
        } finally {
            if (failed) {
                log.info("The task finished by failure.");
                taskFailureNotifier.failed();
            } else {
                log.info("The task completed as succeded.");
            }
        }
    }

    @Override
    public String toString() {
        return "Task [datasetName=" + datasetName + ", viewName=" + viewName + ", viewQuery=" + viewQuery + "]";
    }

}
