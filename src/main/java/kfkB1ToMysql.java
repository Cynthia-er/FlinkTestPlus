import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class kfkB1ToMysql {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port",8883);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //设置checkpoint
        env.enableCheckpointing( 2000, CheckpointingMode.EXACTLY_ONCE);  // 传入两个最基本ck参数；间隔时长，ck模式
        CheckpointConfig checkpointConfig =env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");

        //读取kafka中的数据
        //1、得到KafkaSource算子
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setTopics("test1")
                .setGroupId("g1")
                .build();

        //2、将KafkaSource算子添加到作业流中
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kfk_source");

//        streamSource.print();
        //将流中的数据转化为对象
        SingleOutputStreamOperator<Bean1> bean1 = streamSource.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });

        //将类型为Bean1的流转成表
        tenv.createTemporaryView("bean1",bean1);

//        tenv.executeSql("select * from bean1").print();

        //建表来映射mysql中的flinkTest.table
        tenv.executeSql("create table t_mysql                             "
                +                     "(                                      "
                +                     "   id int,                 "
                +                     "   gender string,                       "
                +                     " PRIMARY KEY (id) NOT ENFORCED"
                +                     ")                                      "
                + "with (                                                     "
                + "      'connector' = 'jdbc',                                "
                + "      'url' = 'jdbc:mysql://hadoop102:3306/flinkTest',     "
                + "      'table-name' = 't1',                                "
                + "      'username' = 'root',                                "
                + "      'password' = '123456'                                "
                + ")                                                          "
        );

        //将表中数据插入到MySQL
        tenv.executeSql("select * from bean1").print();
//        tenv.executeSql("insert into t_mysql select * from bean1");

        env.execute();
    }
}
