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
    public ArrayList<String> getSqlColumn() {
        ArrayList<String> sqlColumns = new ArrayList<>();
        sqlColumns.add(idIndex.sqlCol);
        for (fieldMap fieldMap : fieldList) {
            sqlColumns.add(fieldMap.sqlCol);
        }
        return sqlColumns;
    }

}
