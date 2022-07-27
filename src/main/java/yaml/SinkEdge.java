package yaml;

import java.util.List;

public class SinkEdge {
    public String edgeName;
    public String graphSpace;
    public String graphAddress;
    public String metaAddress;
    public String sourceSql;
    public String sourceDatabase;
    public String sourceTable;
    public Integer srcIndex;
    public Integer dstIndex;
    public Integer rankIndex;
    public List<fieldMap> fieldList;

    public String getEdgeName() {
        return edgeName;
    }

    public void setEdgeName(String edgeName) {
        this.edgeName = edgeName;
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

    public Integer getSrcIndex() {
        return srcIndex;
    }

    public void setSrcIndex(Integer srcIndex) {
        this.srcIndex = srcIndex;
    }

    public Integer getDstIndex() {
        return dstIndex;
    }

    public void setDstIndex(Integer dstIndex) {
        this.dstIndex = dstIndex;
    }

    public Integer getRankIndex() {
        return rankIndex;
    }

    public void setRankIndex(Integer rankIndex) {
        this.rankIndex = rankIndex;
    }

    public List<fieldMap> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<fieldMap> fieldList) {
        this.fieldList = fieldList;
    }
}
