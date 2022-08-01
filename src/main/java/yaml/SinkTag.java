package yaml;

import java.util.ArrayList;
import java.util.List;

public class SinkTag extends AbstractSinkType {
    public final String TYPE = "tag";
    public NebulaPropIndex idIndex;

    public NebulaPropIndex getIdIndex() {
        return idIndex;
    }

    public void setIdIndex(NebulaPropIndex idIndex) {
        this.idIndex = idIndex;
    }

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
        if (!sqlColumns.contains(idIndex.sqlCol)) {
            sqlColumns.add(idIndex.position, idIndex.sqlCol);
        }
        return sqlColumns;
    }

}
