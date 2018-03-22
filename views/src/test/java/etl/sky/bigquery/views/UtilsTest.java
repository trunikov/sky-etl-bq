package etl.sky.bigquery.views;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

public class UtilsTest {

    @Test
    public void prepareViewDdl() {
        String s = Utils.prepareViewDdl("Select \n" + 
                "'test' test_field_1,\n" + 
                "current_timestamp()  test_field_2\n" + 
                "from `PROJECT_NAME_PLACEHOLDER.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" + 
                "limit 1", "123-4545");
        assertNotNull(s);
        assertEquals(s, "Select \n" + 
                "'test' test_field_1,\n" + 
                "current_timestamp()  test_field_2\n" + 
                "from `123-4545.SOURCE_UK.DIM_ACCOUNT_STATUS` \n" + 
                "limit 1");
    }

}
