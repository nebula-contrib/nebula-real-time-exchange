import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.constructor.Constructor;
import yaml.Mysql2NebulaConfig;
import yaml.MysqlSourceIn;

import java.io.InputStream;

public class ConfParseTest {
    public static void main(String[] args) {
        Yaml configYaml = new Yaml(new Constructor(Mysql2NebulaConfig.class));
        InputStream configInput = Runner.class
                .getClassLoader()
                .getResourceAsStream(args[0]);
        Mysql2NebulaConfig mysql2NebulaConfig = configYaml.loadAs(configInput, Mysql2NebulaConfig.class);
        for (MysqlSourceIn mysqlSourceIn : mysql2NebulaConfig.mysqlSourceInList) {
            System.out.println(mysqlSourceIn.address);
        }


    }
    public static void loadYAMLTest(String configPath) {
        Yaml yaml = new Yaml(new Constructor(Mysql2NebulaConfig.class));

        final InputStream inputStream = ConfParseTest.class
                .getClassLoader()
                .getResourceAsStream(configPath);


        Mysql2NebulaConfig load = yaml.loadAs(inputStream, Mysql2NebulaConfig.class);

        System.out.println(load);
    }

}
