package etl.sky.bigquery.merge;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.net.URISyntaxException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.cloud.storage.BlobId;

public class UtilsTest {

    @DataProvider(name = "gsUrls")
    public Object[][] gsUrls() {
        //@formatter:off
        return new Object[][] {
            { "gs://backet1/file1.json", "backet1", "file1.json" },
            { "gs://skyetl-configs/etl_test_view.json", "skyetl-configs", "etl_test_view.json" }
        };
        //@formatter:on
    }

    @Test(dataProvider = "gsUrls")
    public void testFromUrl(String url, String expectedBacket, String expectedName) throws Exception {
        BlobId bid = Utils.fromUrl(url);
        assertNotNull(bid);
        assertEquals(bid.getBucket(), expectedBacket);
        assertEquals(bid.getName(), expectedName);
    }

    @Test(expectedExceptions = { NullPointerException.class })
    public void testFromUrlNPE() throws Exception {
        Utils.fromUrl(null);
    }

    @Test(expectedExceptions = { URISyntaxException.class })
    public void testFromUrlInvalidURI() throws Exception {
        Utils.fromUrl("gs:");
    }

    @Test
    public void testPreparePttrnSql() {
        String s = Utils.preparePttrnSql(" SELECT 0000 bacth_id, a.* FROM [ETL_VIEW.etl_test_view] a ",
                "63319191-8723-4f8f-9f78-14651f963083");
        assertNotNull(s);
        assertEquals(s, "SELECT '63319191-8723-4f8f-9f78-14651f963083' bacth_id, a.* FROM [ETL_VIEW.etl_test_view] a");
    }

    @Test(expectedExceptions = { NullPointerException.class })
    public void testPreparePttrnSqlPttrnSqlInvalidArg() {
        Utils.preparePttrnSql(null, "foo");
    }

    @Test(expectedExceptions = { NullPointerException.class })
    public void testPreparePttrnSqlBatchIdInvalidArg() {
        Utils.preparePttrnSql("foo", null);
    }

    @Test
    public void testPrepareSqlNoBatchId() {
        String s = Utils.preparePttrnSql("SELECT a.* FROM [ETL_VIEW.etl_test_view] a", "foo");
        assertNotNull(s);
        assertEquals(s, "SELECT a.* FROM [ETL_VIEW.etl_test_view] a");
    }

}
