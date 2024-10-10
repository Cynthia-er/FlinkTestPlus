import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Random;

public class B3ToKafka {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8884);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
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
        DataStreamSource<String> bean3Stream = env.addSource(new SourceFunction<String>() {
            private boolean isRunning = true; // 控制源的运行
            private int counter = 1; // 序号计数器
            int MAX_SCORE = 100; // 最大分数

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    // 生成随机分数
                    int score = random.nextInt(MAX_SCORE + 1); // 生成0到100之间的随机整数
                    // 生成带序号的输出
                    String output = counter + ", " + score;
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
                                .setTopic("test3")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        bean3Stream.sinkTo(kafkaSink);

    env.execute();
}
}
