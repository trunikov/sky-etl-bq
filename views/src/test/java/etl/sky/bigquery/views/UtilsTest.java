package etl.sky.bigquery.views;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UtilsTest {

    @DataProvider(name = "dataProviderPrepareViewDdl")
    private Object[][] dataProviderPrepareViewDdl() {
        //@formatter:off
        return new Object[][] {
            new Object[] {
                "Select \n" +
                "'test' test_field_1,\n" + 
                "current_timestamp()  test_field_2\n" +
                "from `PROJECT_NAME_PLACEHOLDER.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" +
                "limit 1",
                "123-4545",
                "Select \n" +
                "'test' test_field_1,\n" +
                "current_timestamp()  test_field_2\n" +
                "from `123-4545.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" +
                "limit 1"
            },
            new Object[] {
                "select \n" + 
                "cast(ACCSTA_ACCOUNT_STATUS_ID as INT64) ACCSTA_ACCOUNT_STATUS_ID, \n" + 
                "ACCSTA_ACCOUNT_STATUS_DESC, \n" + 
                "ACCSTA_EFFECTIVE_FROM_DTTM, \n" + 
                "ACCSTA_EFFECTIVE_TO_DTTM, \n" + 
                "ACCSTA_CREATE_DTTM, \n" + 
                "ACCSTA_LASTUPDATE_DTTM, \n" + 
                "ACCSTA_SOURCE_SYSTEM_NAME, \n" + 
                "ACCSTA_CURRENT_RECORD_IND, \n" + 
                "BATCH_ID,\n" + 
                "'UK' SOURCE\n" + 
                "from `PROJECT_NAME_PLACEHOLDER.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" + 
                "union all\n" + 
                "select \n" + 
                "cast(ACCSTA_ACCOUNT_STATUS_ID as INT64) ACCSTA_ACCOUNT_STATUS_ID, \n" + 
                "ACCSTA_ACCOUNT_STATUS_DESC, \n" + 
                "ACCSTA_EFFECTIVE_FROM_DTTM, \n" + 
                "ACCSTA_EFFECTIVE_TO_DTTM, \n" + 
                "ACCSTA_CREATE_DTTM, \n" + 
                "ACCSTA_LASTUPDATE_DTTM, \n" + 
                "ACCSTA_SOURCE_SYSTEM_NAME, \n" + 
                "ACCSTA_CURRENT_RECORD_IND, \n" + 
                "BATCH_ID,\n" + 
                "'ROI' SOURCE\n" + 
                "from `PROJECT_NAME_PLACEHOLDER.SOURCE_ROI.DIM_ACCOUNT_STATUS` r\n" + 
                "where not exists (select 1\n" + 
                "from `PROJECT_NAME_PLACEHOLDER.SOURCE_UK.DIM_ACCOUNT_STATUS` u\n" + 
                "where u. ACCSTA_ACCOUNT_STATUS_ID = r. ACCSTA_ACCOUNT_STATUS_ID)",
                "foo",
                "select \n" + 
                "cast(ACCSTA_ACCOUNT_STATUS_ID as INT64) ACCSTA_ACCOUNT_STATUS_ID, \n" + 
                "ACCSTA_ACCOUNT_STATUS_DESC, \n" + 
                "ACCSTA_EFFECTIVE_FROM_DTTM, \n" + 
                "ACCSTA_EFFECTIVE_TO_DTTM, \n" + 
                "ACCSTA_CREATE_DTTM, \n" + 
                "ACCSTA_LASTUPDATE_DTTM, \n" + 
                "ACCSTA_SOURCE_SYSTEM_NAME, \n" + 
                "ACCSTA_CURRENT_RECORD_IND, \n" + 
                "BATCH_ID,\n" + 
                "'UK' SOURCE\n" + 
                "from `foo.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" + 
                "union all\n" + 
                "select \n" + 
                "cast(ACCSTA_ACCOUNT_STATUS_ID as INT64) ACCSTA_ACCOUNT_STATUS_ID, \n" + 
                "ACCSTA_ACCOUNT_STATUS_DESC, \n" + 
                "ACCSTA_EFFECTIVE_FROM_DTTM, \n" + 
                "ACCSTA_EFFECTIVE_TO_DTTM, \n" + 
                "ACCSTA_CREATE_DTTM, \n" + 
                "ACCSTA_LASTUPDATE_DTTM, \n" + 
                "ACCSTA_SOURCE_SYSTEM_NAME, \n" + 
                "ACCSTA_CURRENT_RECORD_IND, \n" + 
                "BATCH_ID,\n" + 
                "'ROI' SOURCE\n" + 
                "from `foo.SOURCE_ROI.DIM_ACCOUNT_STATUS` r\n" + 
                "where not exists (select 1\n" + 
                "from `foo.SOURCE_UK.DIM_ACCOUNT_STATUS` u\n" + 
                "where u. ACCSTA_ACCOUNT_STATUS_ID = r. ACCSTA_ACCOUNT_STATUS_ID)"
            }
        };
        //@formatter:on
    }

    @Test(dataProvider = "dataProviderPrepareViewDdl")
    public void prepareViewDdl(String pttrnSql, String projectId, String expected) {
        String s = Utils.prepareViewDdl(pttrnSql, projectId);
        assertNotNull(s);
        assertEquals(s, expected);
    }

    @DataProvider(name = "dataProviderParseFilename")
    private Object[][] dataProviderParseFilename() {
        //@formatter:off
        return new Object[][] {
            new Object[] { "ETL_VIEW.etl_test_view.txt", Pair.of("ETL_VIEW", "etl_test_view") },
            new Object[] { "etl_ddl/ETL_VIEW.etl_test_view.txt", Pair.of("ETL_VIEW", "etl_test_view") },
            new Object[] { "etl_ddl/foo/ETL_VIEW.etl_test_view.txt", Pair.of("ETL_VIEW", "etl_test_view") },
            new Object[] { "etl_ddl/foo/ETL_VIEW.etl_test_view/bar.txt", Pair.of("ETL_VIEW", "etl_test_view/bar") }
        };
        //@formatter:on
    }

    @Test(dataProvider = "dataProviderParseFilename")
    public void testParseBlobName(String filename, Pair<String, String> expected) {
        Pair<String, String> result = Utils.parseBlobName(filename);
        assertEquals(result, expected);
    }

}
