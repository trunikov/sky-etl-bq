package etl.sky.bigquery.views;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * @author dmytro.trunykov@gmail.com
 */
public class App implements Callable<Void> {
    
    private final static Logger log = LoggerFactory.getLogger(App.class);

    private AppConfig opts;

    App(AppConfig opts) {
        this.opts = opts;
    }

    @Override
    public Void call() throws Exception {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Page<Blob> blobs = storage.list(opts.getBucketName());
        for(Blob b : blobs.iterateAll()) {
            log.debug("Blob: {}", b);
            byte[] bytes = storage.readAllBytes(b.getBlobId());
            String pttrnSql = new String(bytes, "UTF-8");
            String sql = Utils.prepareViewDdl(pttrnSql, opts.getProjectId());
            log.info("sql: {}", sql);
        }
        return null;
    }

}
