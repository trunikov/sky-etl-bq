package etl.sky.bigquery.merge;


import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskConfig {

    @JsonProperty("job_type")
    private String jobType;

    @JsonProperty("job_order")
    private Double jobOrder;

    @JsonProperty("dataset")
    private String dataSet;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("sql")
    private String sqlPttrn;

    @JsonProperty("createDisposition")
    private String createDisposition;

    @JsonProperty("writeDisposition")
    private String writeDisposition;

    public TaskConfig() {
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public Double getJobOrder() {
        return jobOrder;
    }

    public void setJobOrder(Double jobOrder) {
        this.jobOrder = jobOrder;
    }

    public String getDataSet() {
        return dataSet;
    }

    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSqlPttrn() {
        return sqlPttrn;
    }

    public void setSqlPttrn(String sqlPttrn) {
        this.sqlPttrn = sqlPttrn;
    }

    public String getCreateDisposition() {
        return createDisposition;
    }

    public void setCreateDisposition(String createDisposition) {
        this.createDisposition = createDisposition;
    }

    public String getWriteDisposition() {
        return writeDisposition;
    }

    public void setWriteDisposition(String writeDisposition) {
        this.writeDisposition = writeDisposition;
    }

    @Override
    public String toString() {
        return "TaskConfig [jobType=" + jobType + ", jobOrder=" + jobOrder + ", dataSet=" + dataSet + ", tableName="
                + tableName + ", sqlPttrn=" + sqlPttrn + ", createDisposition=" + createDisposition
                + ", writeDisposition=" + writeDisposition + "]";
    }

}
