import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FormatTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        List<List<String>> friend = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "62", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields2 = Arrays.asList("62", "63", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields3 = Arrays.asList("63", "64", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields4 = Arrays.asList("64", "65", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields5 = Arrays.asList("65", "66", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields6 = Arrays.asList("66", "67", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields7 = Arrays.asList("67", "68", "李四", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0",
                "11:12:12", "polygon((0 1,1 2,2 3,0 1))");
        List<String> fields8 = Arrays.asList("68", "61", "aba", "张三", "1", "1111", "22222",
                "6412233",
                "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0", "11:12:12",
                "POLYGON((0 1,1 2,2 3,0 1))");
        friend.add(fields1);
        friend.add(fields2);
        friend.add(fields3);
        friend.add(fields4);
        friend.add(fields5);
        friend.add(fields6);
        friend.add(fields7);
        friend.add(fields8);
        DataStream<List<String>> playerSource = env.fromCollection(friend);
        System.out.println(friend);
    }
}
