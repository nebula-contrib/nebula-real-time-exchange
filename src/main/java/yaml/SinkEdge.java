package yaml;


import java.util.ArrayList;
import java.util.List;

public class SinkEdge extends AbstractSinkType {
    public final String TYPE = "edge";
    public NebulaPropIndex srcIndex;
    public NebulaPropIndex dstIndex;
    public NebulaPropIndex rankIndex;

    @Override
    public String getSinkType() {
        return this.TYPE;
    }

    @Override
    public ArrayList<String> getSqlColumn() {
        ArrayList<String> sqlColumns = new ArrayList<>();
        sqlColumns.add(srcIndex.sqlCol);
        sqlColumns.add(dstIndex.sqlCol);
        sqlColumns.add(rankIndex.sqlCol);
        for (fieldMap fieldMap : fieldList) {
            sqlColumns.add(fieldMap.sqlCol);
        }
        return sqlColumns;
    }

    public NebulaPropIndex getSrcIndex() {
        return srcIndex;
    }

    public void setSrcIndex(NebulaPropIndex srcIndex) {
        this.srcIndex = srcIndex;
    }

    public NebulaPropIndex getDstIndex() {
        return dstIndex;
    }

    public void setDstIndex(NebulaPropIndex dstIndex) {
        this.dstIndex = dstIndex;
    }

    public NebulaPropIndex getRankIndex() {
        return rankIndex;
    }

    public void setRankIndex(NebulaPropIndex rankIndex) {
        this.rankIndex = rankIndex;
    }
}
