import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class testCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启checkpoint
//        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/checkpoint");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //使用FlinkCDC构造MySQLSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("flinkTest")
                .tableList("flinkTest.t1")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        //读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"mysql-source");

        //打印
        mysqlDS.print();


        //用cdc连接器获取到MySQL中变更的数据
//        tenv.executeSql("CREATE TABLE t("
//                        +  "       id int,                 "
//                        +  "       gender string,                       "
//                        +  "       primary key(id) not enforced                       "
//                        +  "  ) WITH ("
//                        +  "            'connector' = 'mysql-cdc',"
//                        +  "            'hostname' = 'hadoop102',"
//                        +  "            'port' = '3306',"
//                        +  "            'username' = 'root',"
//                        +  "            'password' = '123456',"
//                        +  "            'database-name' = 'flinkTest',"
//                        +  "            'table-name' = 'flinkTest.t1',"
//                        +  "            'debezium.database.tablename.case.insensitive' = 'false',"
//                        +  "            'debezium.database.serverTimezone' = 'Asia/Shanghai',"
//                        +  "            'debezium.log.mining.strategy' = 'online_catalog'"
//                +  "  )");
//
//        tenv.executeSql("select id,gender from t").print();

        env.execute();
    }
}
