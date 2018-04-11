package etl.sky.bigquery.merge;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.cloud.storage.BlobId;

/**
 * @author dmytry.trunykov@zorallabs.com
 */
public class Utils {

    private final static String PLACEHOLDER = "0000";

    /**
     * Parse URL to a file in Google Cloud Storage.
     * 
     * The URL is expected in a form: gs://<BUCKET_NAME>/<FILE_NAME>
     * 
     * See also https://github.com/GoogleCloudPlatform/google-cloud-java/issues/2397
     * 
     * @param url
     * @return
     * @throws URISyntaxException
     */
    public static BlobId fromUrl(String url) throws URISyntaxException {
        if (url == null) {
            throw new NullPointerException("URL can't be null.");
        }
        Pattern pattern = Pattern.compile("^\\s*gs:\\/\\/([\\da-zA-Z\\.\\-_]+)/([\\da-zA-Z\\.\\-_/]+)+\\s*$");
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String bucket = matcher.group(1);
            String name = matcher.group(2);
            return BlobId.of(bucket, name);
        } else {
            throw new URISyntaxException(url, "Invalid URL to a file on Google Cloud Storage.");
        }
    }

    public static String preparePttrnSql(String pttrnSql, String batchId) throws IllegalArgumentException {
        if (pttrnSql == null) {
            throw new NullPointerException("Mandatory input argument 'pttrnSql' can't be null.");
        }
        if (batchId == null) {
            throw new NullPointerException("Mandatory input argument 'batchId' can't be null.");
        }
        pttrnSql = pttrnSql.trim();
        String sql;
        int m = pttrnSql.indexOf(" " + PLACEHOLDER + " ");
        if (m == -1) {
            sql = pttrnSql;
        } else {
            sql = pttrnSql.substring(0, m) + " '" + batchId + "' " + pttrnSql.substring(m + PLACEHOLDER.length() + 2);
        }
        return sql;
    }

}
