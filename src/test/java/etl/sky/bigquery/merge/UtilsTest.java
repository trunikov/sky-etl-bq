package etl.sky.bigquery.merge;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.net.URISyntaxException;

import org.testng.annotations.Test;

import com.google.cloud.storage.BlobId;

public class UtilsTest {

    @Test
    public void testFromUrl() throws Exception {
//        Utils.fromUrl("gs://skyetl-configs/etl_test_view.json");
        BlobId bid = Utils.fromUrl("gs://backet1/file1.json");
        assertNotNull(bid);
        assertEquals(bid.getBucket(), "backet1");
        assertEquals(bid.getName(), "file1.json");
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

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public void testPrepareSqlInvalidPttrn() {
        Utils.preparePttrnSql("SELECT a.* FROM [ETL_VIEW.etl_test_view] a", "foo");
    }

}
