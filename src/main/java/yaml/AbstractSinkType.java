package yaml;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSinkType {
    public String sinkName;
    public String graphSpace;
    public String graphAddress;
    public String metaAddress;
    public String sourceSql;
    public String sourceDatabase;
    public String sourceTable;
    public List<fieldMap> fieldList;

    public abstract String getSinkType();

    public ArrayList<String> getFields() {
        ArrayList<String> fields = new ArrayList<>();
        for (fieldMap fieldMap : fieldList) {
            fields.add(fieldMap.name);
        }
        return fields;
    }

    public ArrayList<Integer> getPositions() {
        ArrayList<Integer> positions = new ArrayList<>();
        for (fieldMap fieldMap : fieldList) {
            positions.add(fieldMap.position);
        }
        return positions;
    }

    public abstract ArrayList<String> getSqlColumns();


    public String getSinkName() {
        return sinkName;
    }

    public void setSinkName(String sinkName) {
        this.sinkName = sinkName;
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

    public List<fieldMap> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<fieldMap> fieldList) {
        this.fieldList = fieldList;
    }
}
