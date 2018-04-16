package etl.sky.bigquery.merge;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
public class App implements Callable<Void> {

    private final Logger log = LoggerFactory.getLogger(App.class);

    private final AppConfig opts;

    App(AppConfig opts) {
        this.opts = opts;
    }

    @Override
    public Void call() throws Exception {
        List<TaskConfig> tasksConfigs = loadConfig(opts.getUrlConfig());
        executeTasks(opts.getBatchId(), tasksConfigs);
        return null;
    }

    private List<TaskConfig> loadConfig(String url) throws IOException, StorageException, URISyntaxException {
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

    private void executeTasks(String batchId, List<TaskConfig> tasksConfigs) throws JobException, InterruptedException {
        List<Task> tasks = tasksConfigs.stream().map(tc -> {
            log.info("A task is being created for the config: {}", tc);
            return Task.of(batchId, tc);
        }).collect(Collectors.toList());
        ExecutorService executorService = Executors.newFixedThreadPool(opts.getThreadPoolSize());
        executorService.invokeAll(tasks);
        executorService.shutdown();
    }

}
