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
    public ArrayList<String> getSqlColumns() {
        ArrayList<String> sqlColumns = new ArrayList<>();
        for (fieldMap fieldMap : fieldList) {
            sqlColumns.add(fieldMap.sqlCol);
        }
        if (!sqlColumns.contains(srcIndex.sqlCol)) {
            sqlColumns.add(srcIndex.position, srcIndex.sqlCol);
        }
        if (!sqlColumns.contains(dstIndex.sqlCol)) {
            sqlColumns.add(dstIndex.position, dstIndex.sqlCol);
        }
        if (!sqlColumns.contains(rankIndex.sqlCol)) {
            sqlColumns.add(rankIndex.position, rankIndex.sqlCol);
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
