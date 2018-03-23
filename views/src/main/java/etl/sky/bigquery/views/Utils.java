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

}
