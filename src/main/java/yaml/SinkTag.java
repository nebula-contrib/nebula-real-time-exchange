package yaml;

import java.util.List;

public class SinkTag {

    public String tagName;
    public String graphSpace;
    public String graphAddress;
    public String metaAddress;
    public String sourceSql;
    public String sourceDatabase;
    public String sourceTable;
    public Integer idIndex;
    public List<fieldMap> fieldList;

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public String getGraphSpace() {
        return graphSpace;
    }

    public void setGraphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
    }

    public String getGraphAddress() {
        return graphAddress;
    }

    public void setGraphAddress(String graphAddress) {
        this.graphAddress = graphAddress;
    }

    public String getMetaAddress() {
        return metaAddress;
    }

    public void setMetaAddress(String metaAddress) {
        this.metaAddress = metaAddress;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public Integer getIdIndex() {
        return idIndex;
    }

    public void setIdIndex(Integer idIndex) {
        this.idIndex = idIndex;
    }

    public List<fieldMap> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<fieldMap> fieldList) {
        this.fieldList = fieldList;
    }
}
