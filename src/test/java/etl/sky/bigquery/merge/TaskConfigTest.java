package etl.sky.bigquery.merge;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TaskConfigTest {

    @Test
    public void testDeserialization() throws JsonParseException, JsonMappingException, IOException {
        String s = "{  \n" + 
                "  \"job_type\":\"ETL\",\n" + 
                "  \"job_order\":0.1,  \n" + 
                "  \"dataset\":\"ETL_VIEW\",\n" + 
                "  \"table_name\":\"etl_test_view_table\",\n" + 
                "  \"sql\":\"SELECT 0000 bacth_id, a.* FROM [ETL_VIEW.etl_test_view] a\",\n" + 
                "  \"createDisposition\":\"CREATE_IF_NEEDED\",\n" + 
                "  \"writeDisposition\":\"WRITE_APPEND\"\n" + 
                "\n" + 
                "}";
        ObjectMapper mapper = new ObjectMapper();
        //TaskConfig tc = new TaskConfig();
        //s = mapper.writeValueAsString(tc);
        //System.out.println(s);
        TaskConfig tc = mapper.readValue(s, TaskConfig.class);
        assertNotNull(tc);
        assertEquals(tc.getJobType(), "ETL");
        assertEquals(tc.getJobOrder(), 0.1D);
        assertEquals(tc.getDataSet(), "ETL_VIEW");
        assertEquals(tc.getTableName(), "etl_test_view_table");
        assertEquals(tc.getSqlPttrn(), "SELECT 0000 bacth_id, a.* FROM [ETL_VIEW.etl_test_view] a");
        assertEquals(tc.getCreateDisposition(), "CREATE_IF_NEEDED");
        assertEquals(tc.getWriteDisposition(), "WRITE_APPEND");
    }

}
