import com.vesoft.nebula.jdbc.impl.NebulaDriver;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.sql.*;

import org.slf4j.Logger;

import static org.junit.Assert.*;

public class RunnerTest {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    NebulaDriver nebulaDriver;
    Connection nebulaConn;
    Connection mysqlConn;
    Statement nebulaStmt;
    //    Statement mysqlStmt;
    PreparedStatement mysqlInsertTagStmt;
    PreparedStatement mysqlUpdateTagStmt;
    PreparedStatement mysqlDeleteTagStmt;
    PreparedStatement mysqlInsertEdgeStmt;
    PreparedStatement mysqlUpdateEdgeStmt;
    PreparedStatement mysqlDeleteEdgeStmt;
    PreparedStatement tagQuery;
    PreparedStatement edgeQuery;

    static final String DB_URL = "jdbc:mysql://localhost:3306/test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    @Before
    public void init() throws SQLException, ClassNotFoundException {
        nebulaDriver = new NebulaDriver("127.0.0.1:9669");
        nebulaConn = DriverManager.getConnection("jdbc:nebula://basketballplayer", "root", "nebula123");
        nebulaStmt = nebulaConn.createStatement();
        Class.forName("com.mysql.cj.jdbc.Driver");
        log.info("Connecting to a selected MySQL database...");
        mysqlConn = DriverManager.getConnection(DB_URL, "root", "162331");
//        mysqlStmt = mysqlConn.createStatement();
        log.info("Connected MySQL successfully...");
        mysqlInsertTagStmt = mysqlConn.prepareStatement("INSERT INTO player (playerid, name, age) VALUES(?,?,?)");
        mysqlUpdateTagStmt = mysqlConn.prepareStatement("UPDATE player SET age = ? WHERE playerid = ?");
        mysqlDeleteTagStmt = mysqlConn.prepareStatement("DELETE FROM player WHERE playerid = ?");
        mysqlInsertEdgeStmt = mysqlConn.prepareStatement("INSERT INTO follow (src_player, dst_player, degree) VALUES(?,?,?)");
        mysqlUpdateEdgeStmt = mysqlConn.prepareStatement("UPDATE follow SET degree = ? WHERE src_player = ? AND dst_player = ?");
        mysqlDeleteEdgeStmt = mysqlConn.prepareStatement("DELETE FROM follow WHERE src_player = ? AND dst_player = ?");
        tagQuery = nebulaConn.prepareStatement("MATCH (v:player) WHERE id(v) == ? RETURN v.player.name AS pName, v.player.age AS pAge");
        edgeQuery = nebulaConn.prepareStatement(" FETCH PROP ON follow ? -> ? YIELD properties(edge).degree AS fDegree");

    }

    @Test
    public void checkTagTest() throws SQLException, IOException, InterruptedException {
        //开启同步任务后执行测试
        BufferedReader vplayerReader = new BufferedReader(new FileReader("src/test/java/testDataset/vertex_player.csv"));
        String line;
        while ((line = vplayerReader.readLine()) != null) {
            String item[] = line.split(",");
            String exceptedPid = item[0];
            int exceptedAge = Integer.parseInt(item[1]);
            String exceptedName = item[2];
            log.info("Running Test Tag: " + exceptedPid + " " + exceptedName + " " + exceptedAge);
            mysqlInsertTagStmt.setString(1, exceptedPid);
            mysqlInsertTagStmt.setString(2, exceptedName);
            mysqlInsertTagStmt.setInt(3, exceptedAge);
            mysqlInsertTagStmt.executeUpdate();
            tagQuery.setString(1, exceptedPid);
            Thread.sleep(1000L);
            ResultSet insertResultSet = tagQuery.executeQuery();
            assertNotNull(insertResultSet);
//            assertTrue(insertResultSet.next());
            assertEquals(exceptedAge, insertResultSet.getInt("pAge"));
            assertEquals(exceptedName, insertResultSet.getString("pName"));

            mysqlUpdateTagStmt.setInt(1, exceptedAge + 1);
            mysqlUpdateTagStmt.setString(2, exceptedPid);
            mysqlUpdateTagStmt.executeUpdate();
            Thread.sleep(1000L);
            ResultSet updateResultSet = tagQuery.executeQuery();
            assertNotNull(updateResultSet);
            assertTrue(updateResultSet.next());
            assertEquals(exceptedAge + 1, updateResultSet.getInt("pAge"));
            assertEquals(exceptedName, updateResultSet.getString("pName"));

            mysqlDeleteTagStmt.setString(1, exceptedPid);
            mysqlDeleteTagStmt.executeUpdate();
            Thread.sleep(1000L);
            ResultSet deleteResultSet = tagQuery.executeQuery();
            assertFalse(deleteResultSet.next());
        }
    }

    @Test
    public void checkEdgeTest() throws SQLException, IOException, InterruptedException {
        //开启同步任务后执行测试
        BufferedReader efollowReader = new BufferedReader(new FileReader("src/test/java/testDataset/edge_follow.csv"));
        String line;
        while ((line = efollowReader.readLine()) != null) {
            String item[] = line.split(",");
            String exceptedSrcPid = item[0];
            String exceptedDstPid = item[1];
            int exceptedDegree = Integer.parseInt(item[2]);

            mysqlInsertTagStmt.setString(1,exceptedSrcPid);
            mysqlInsertTagStmt.setString(2, "exceptedName");
            mysqlInsertTagStmt.setInt(3, 10);
            mysqlInsertTagStmt.executeUpdate();
            mysqlInsertTagStmt.setString(1,exceptedDstPid);
            mysqlInsertTagStmt.executeUpdate();

            log.info("Running Test Edge: " + exceptedSrcPid + " " + exceptedDstPid + " " + exceptedDegree);
            mysqlInsertEdgeStmt.setString(1, exceptedSrcPid);
            mysqlInsertEdgeStmt.setString(2, exceptedDstPid);
            mysqlInsertEdgeStmt.setInt(3, exceptedDegree);
            mysqlInsertEdgeStmt.executeUpdate();

            edgeQuery.setString(1, exceptedSrcPid);
            edgeQuery.setString(2, exceptedDstPid);
            Thread.sleep(1000L);
            ResultSet insertResultSet = edgeQuery.executeQuery();
            assertNotNull(insertResultSet);
            assertTrue(insertResultSet.next());
            assertEquals(exceptedDegree, insertResultSet.getInt("fDegree"));

            mysqlUpdateEdgeStmt.setInt(1, exceptedDegree + 1);
            mysqlUpdateEdgeStmt.setString(2, exceptedSrcPid);
            mysqlUpdateEdgeStmt.setString(3, exceptedDstPid);
            mysqlUpdateEdgeStmt.executeUpdate();
            Thread.sleep(1000L);
            ResultSet updateResultSet = edgeQuery.executeQuery();
            assertNotNull(updateResultSet);
            assertTrue(updateResultSet.next());
            assertEquals(exceptedDegree + 1, updateResultSet.getInt("fDegree"));


            mysqlDeleteEdgeStmt.setString(1, exceptedSrcPid);
            mysqlDeleteEdgeStmt.setString(2, exceptedDstPid);
            mysqlDeleteEdgeStmt.executeUpdate();
            Thread.sleep(1000L);
            ResultSet deleteResultSet = edgeQuery.executeQuery();
            assertFalse(deleteResultSet.next());
            mysqlDeleteTagStmt.setString(1, exceptedSrcPid);
            mysqlDeleteTagStmt.execute();
            mysqlDeleteTagStmt.setString(1, exceptedDstPid);
            mysqlDeleteTagStmt.execute();
        }
    }
}
