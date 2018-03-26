package etl.sky.bigquery.views;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

/**
 * @author dmytro.trunykov@zorallabs.com
 */
public class Utils {

    private final static String PLACEHOLDER = "PROJECT_NAME_PLACEHOLDER";

    public static String prepareViewDdl(String pttrnSql, String projectId) {
        if (pttrnSql == null) {
            throw new NullPointerException("Mandatory input argument 'pttrnSql' can't be null.");
        }
        if (projectId == null) {
            throw new NullPointerException("Mandatory input argument 'projectId' can't be null.");
        }
        pttrnSql = pttrnSql.trim();
        String sql;
        int m = pttrnSql.indexOf("`" + PLACEHOLDER + ".");
        if (m == -1) {
            sql = pttrnSql;
        } else {
            sql = pttrnSql.substring(0, m) + "`" + projectId + "." + pttrnSql.substring(m + PLACEHOLDER.length() + 2);
        }
        return sql;
    }

    /**
     * Parse a string in a form 'gs://<bucket>/<name>' to extract '<bucket>' and '<name>'.
     * 
     * @param url
     *            a string in a form 'gs://<bucket>/<name>'
     * @return a pair where [0] - bucket name, [1] - name
     * @throws URISyntaxException
     */
    public static Pair<String, String> fromUrl(String url) throws URISyntaxException {
        if (url == null) {
            throw new NullPointerException("URL can't be null.");
        }
        Pattern pattern = Pattern.compile("^\\s*gs:\\/\\/([\\da-zA-Z\\.\\-_]+)/([\\da-zA-Z\\.\\-_]+)$");
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches() || matcher.groupCount() != 2) {
            String bucket = matcher.group(1);
            String name = matcher.group(2);
            return Pair.of(bucket, name);
        } else {
            throw new URISyntaxException(url, "Invalid URL to a file on Google Cloud Storage.");
        }
    }

    /**
     * Parse passed filename and split it on 'dataset name' and 'view name'.
     * 
     * This utility expects that input files have names in a predefined form:
     * 
     * <code>
     *   [any characters][/]<dataset name>.<view name>.[<any chars>]
     * </code>
     * 
     * This function parses a passed filename and returns a pair in a form (<dataset name>, <view name>).
     * 
     * @param blobName
     * @return a pair in a form (<dataset name>, <view name>)
     * @throws NullPointerException
     *             when an input argument 'filename' is null
     * @throws IllegalArgumentException
     *             when an input argument can't be parsed
     */
    public static Pair<String, String> parseBlobName(String blobName) {
        if (blobName == null) {
            throw new NullPointerException("Parameter 'filename' can't be null.");
        }
        int p1 = blobName.indexOf('.');
        if (p1 == -1) {
            throw new IllegalArgumentException(
                    "The filename can't be parsed. Name of a dataset not found: " + blobName);
        }
        int p0 = blobName.lastIndexOf('/', p1);
        int p2 = blobName.indexOf('.', p1 + 1);
        if (p2 == -1) {
            throw new IllegalArgumentException("The filename can't be parsed. Name of a view not found: " + blobName);
        }
        String datasetName;
        if (p0 > p1) {
            datasetName = blobName.substring(0, p1);
        } else {
            datasetName = blobName.substring(p0 + 1, p1);
        }
        String viewName = blobName.substring(p1 + 1, p2);
        return Pair.of(datasetName, viewName);
    }

}
