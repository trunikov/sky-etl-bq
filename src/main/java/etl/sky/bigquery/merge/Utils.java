package etl.sky.bigquery.merge;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.cloud.storage.BlobId;

/**
 * @author dmytry.trunykov@zorallabs.com
 */
public class Utils {

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
        Pattern pattern = Pattern.compile("^\\s*gs:\\/\\/([\\da-zA-Z\\.-]+)/([\\da-zA-Z\\.-]+)$");
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches() || matcher.groupCount() != 2) {
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
        int m = pttrnSql.indexOf(" 0000 ");
        if (m == -1) {
            throw new IllegalArgumentException("Invalid SQL pattern. The placeholder '0000' not found.");
        }
        String sql = pttrnSql.substring(0, m) + " '" + batchId + "' " + pttrnSql.substring(m + 6);
        return sql;
    }

}
