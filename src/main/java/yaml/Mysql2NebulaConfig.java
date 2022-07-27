package yaml;

import java.util.List;

public class Mysql2NebulaConfig {
    public List<MysqlSourceIn> mysqlSourceInList;
    public NebulaSink nebulaSink;
    public List<MysqlSourceIn> getMysqlSourceInList() {
        return mysqlSourceInList;
    }

    public void setMysqlSourceInList(List<MysqlSourceIn> mysqlSourceInList) {
        this.mysqlSourceInList = mysqlSourceInList;
    }

    public NebulaSink getNebulaSink() {
        return nebulaSink;
    }

    public void setNebulaSink(NebulaSink nebulaSink) {
        this.nebulaSink = nebulaSink;
    }
}
