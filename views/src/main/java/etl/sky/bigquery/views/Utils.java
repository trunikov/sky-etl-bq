package etl.sky.bigquery.views;

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

}
