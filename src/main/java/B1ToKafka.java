import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class B1ToKafka {
    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port",8882);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // 一个时间段内的最大失败次数
                Time.of(5, TimeUnit.MINUTES), // 衡量失败次数的是时间段
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));

        //设置checkpoint
        env.enableCheckpointing( 2000, CheckpointingMode.EXACTLY_ONCE);  // 传入两个最基本ck参数；间隔时长，ck模式
        CheckpointConfig checkpointConfig =env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


//        tenv.executeSql("CREATE TABLE stu (\n" +
//                           "    id INT,\n" +
//                           "    name        string,\n" +
//                           "    gender       string\n" +
//                           ") WITH (\n" +
//                           "  'connector' = 'datagen',\n" +
//                           "  'fields.id.max' = '1000',\n" +
//                           "  'fields.id.min' = '1',\n" +
//                           "  'fields.name.length' = '9',\n" +
//                           "  'fields.gender.length' = '6',\n" +
//                           "  'rows-per-second' = '1'\n" +
//                           ")"
//        );

        // 创建源，随机产生带序号的"male"和"female"
        DataStreamSource<String> bean1Stream = env.addSource(new SourceFunction<String>() {
            private boolean isRunning = true; // 控制源的运行
            private int counter = 1; // 序号计数器

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    // 随机选择"male"或"female"
                    String gender = random.nextBoolean() ? "male" : "female";
                    // 生成带序号的输出
                    String output = counter + ", " + gender;
                    ctx.collect(output); // 将生成的性别发送到下游
                    counter++; // 序号自增
                    Thread.sleep(1000); // 每秒产生一个新的性别
                }
            }

            @Override
            public void cancel() {
                isRunning = false; // 停止源
            }
        });

//        bean1Stream.print();

//        DataStream<Row> datagenSource = tenv.toChangelogStream(tenv.from("stu"));
//        datagenSource.print();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("test1")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        bean1Stream.sinkTo(kafkaSink);

    env.execute();
}
}
