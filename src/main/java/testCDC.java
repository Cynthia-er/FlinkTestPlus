import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class testCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //开启checkpoint
        env.enableCheckpointing( 2000, CheckpointingMode.EXACTLY_ONCE);  // 传入两个最基本ck参数；间隔时长，ck模式
        CheckpointConfig checkpointConfig =env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");

        //使用FlinkCDC构造MySQLSource
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("hadoop102")
//                .port(3306)
//                .username("root")
//                .password("123456")
//                .databaseList("flinkTest")
//                .tableList("flinkTest.t1")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .build();
//
//        //读取数据
//        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"mysql-source");

        //打印
//        mysqlDS.print();

        //用cdc连接器获取到MySQL中变更的数据
        tenv.executeSql("CREATE TABLE t("
                        +  "       id int,                 "
                        +  "       gender string,                       "
                        +  "       primary key(id) not enforced                       "
                        +  "  ) WITH ("
                        +  "            'connector' = 'mysql-cdc',"
                        +  "            'hostname' = 'hadoop102',"
                        +  "            'port' = '3306',"
                        +  "            'username' = 'root',"
                        +  "            'password' = '123456',"
                        +  "            'database-name' = 'flinkTest',"
                        +  "            'table-name' = 't1'"
                +  "  )");


        tenv.executeSql("create table sink (\n" +
                "                      `id` int,\n" +
                "                      `gender` string,\n" +
                "                      primary key(id) not enforced\n" +
                "                ) with (\n" +
                "                 'connector' = 'jdbc',\n" +
                "                 'url' = 'jdbc:mysql://hadoop102:3306/flinkTest',\n" +
                "                 'username' = 'root',\n" +
                "                 'password' = '123456',\n" +
                "                 'table-name' = 't5'\n" +
                "                );");


//        tenv.executeSql("select * from t").print();
        tenv.executeSql("insert into sink select * from t");

        env.execute();
    }
}
