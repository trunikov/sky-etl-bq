package etl.sky.bigquery.views;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
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
        // BlobId bid = BlobId.of(opts.getBucketName(), opts.getFolderName() + "/ETL_VIEW.etl_test_view.sql");
        BlobId bid = BlobId.of(opts.getBucketName(), opts.getFolderName() + "/es_query.txt");
        Blob blob = storage.get(bid);
        Long size = blob.getSize();
        byte[] buf = storage.readAllBytes(bid);
        /*
        Bucket bucket = storage.get(opts.getBucketName());
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket '" + opts.getBucketName() + "' not found.");
        }
        Page<Blob> blobs = bucket.list(BlobListOption.currentDirectory(), BlobListOption.prefix(opts.getFolderName() + "/"));
        for (Blob b : blobs.iterateAll()) {
            log.debug("Blob: {}", b);
            if (!b.isDirectory()) {
                byte[] bytes0 = b.getContent();
                String s = new String(bytes0, "UTF-8");
                byte[] bytes = storage.readAllBytes(b.getBlobId());
                String pttrnSql = new String(bytes, "UTF-8");
                String sql = Utils.prepareViewDdl(pttrnSql, opts.getProjectId());
                log.info("sql: {}", sql);
            }
        }
        */
        return null;
    }

}
