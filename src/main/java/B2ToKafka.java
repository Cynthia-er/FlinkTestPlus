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

public class B2ToKafka {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8882);

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

        String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int NAME_LENGTH = 6;

        // 创建源，随机产生带序号和名字
        DataStreamSource<String> bean2Stream = env.addSource(new SourceFunction<String>() {
            boolean isRunning = true; // 控制源的运行
            int counter = 1; // 序号计数器
            Random random = new Random(); // 随机数生成器

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    // 生成随机名字
                    String name = generateRandomName(NAME_LENGTH);
                    // 生成带序号的输出
                    String output = counter + ", " + name;
                    ctx.collect(output); // 将生成的名字发送到下游
                    counter++; // 序号自增
                    Thread.sleep(1000); // 每秒产生一个新的名字
                }
            }

            @Override
            public void cancel() {
                isRunning = false; // 停止源
            }

            // 生成随机名字的方法
            private String generateRandomName(int length) {
                StringBuilder nameBuilder = new StringBuilder();
                // 首字母随机大写字母
                nameBuilder.append((char) ('A' + random.nextInt(26)));
                // 余下部分随机小写字母
                for (int i = 1; i < length; i++) {
                    nameBuilder.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
                }
                return nameBuilder.toString();
            }
        });

//        bean1Stream.print();

//        DataStream<Row> datagenSource = tenv.toChangelogStream(tenv.from("stu"));

//        datagenSource.print();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("test2")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        bean2Stream.sinkTo(kafkaSink);

    env.execute();
}
}
