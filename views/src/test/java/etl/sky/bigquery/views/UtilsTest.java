package etl.sky.bigquery.views;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UtilsTest {

    @Test
    public void prepareViewDdl() {
        String s = Utils.prepareViewDdl("Select \n" + "'test' test_field_1,\n" + "current_timestamp()  test_field_2\n"
                + "from `PROJECT_NAME_PLACEHOLDER.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" + "limit 1", "123-4545");
        assertNotNull(s);
        assertEquals(s, "Select \n" + "'test' test_field_1,\n" + "current_timestamp()  test_field_2\n"
                + "from `123-4545.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" + "limit 1");
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
