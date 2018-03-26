package etl.sky.bigquery.views;

import static etl.sky.bigquery.views.Utils.parseBlobName;
import static etl.sky.bigquery.views.Utils.prepareViewDdl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;

/**
 * @author dmytro.trunykov@gmail.com
 */
public class App implements Callable<Void>, TaskFailureNotifier {

    private final static Logger log = LoggerFactory.getLogger(App.class);

    private AppConfig opts;

    /**
     * A flag which can be modified by a spawned task in case of failure.
     * 
     * This flag is also controlled by the main thread to stop spawn new tasks and gracefully exit.
     */
    private volatile boolean failure;

    App(AppConfig opts) {
        this.opts = opts;
    }

    @Override
    public void failed() {
        this.failure = true;
    }

    @Override
    public Void call() throws Exception {
        failure = false;
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get(opts.getBucketName());
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket '" + opts.getBucketName() + "' not found.");
        }
        List<Task> tasks = new ArrayList<>(512);
        Page<Blob> blobs = bucket.list(BlobListOption.prefix(opts.getFolderName() + "/"));
        for (Blob b : blobs.iterateAll()) {
            log.debug("BLOB: {}", b);
            if (failure) {
                log.info("Catched signal about failure. Spawning of new tasks stopped.");
                System.exit(2);
            }
            if ("text/plain".equals(b.getContentType())) {
                String filename = b.getName();
                Pair<String, String> pfn = parseBlobName(filename);
                String datasetName = pfn.getLeft();
                String viewName = pfn.getRight();
                log.debug("datasetName: {}, viewName: {}", datasetName, viewName);
                byte[] ba = b.getContent();
                String pttrnSql = new String(ba, "UTF-8");
                String viewQuery = prepareViewDdl(pttrnSql, opts.getProjectId());
                log.debug("Pattern SQL translation: {} => {}", pttrnSql, viewQuery);
                Task t = Task.of(this, datasetName, viewName, viewQuery);
                tasks.add(t);
            } else {
                log.info("Encountered unsuitable BLOB '{}'. Skipped.", b);
            }
        }
        ExecutorService executorService = Executors.newFixedThreadPool(opts.getThreadPoolSize());
        executorService.invokeAll(tasks);
        executorService.shutdown();
        return null;
    }

}
