import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.Duration;

public class checkpointConfig{

    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置状态后端（例如使用 RocksDB）
        /**
         * MemoryStateBackend（默认）：状态存储在内存中，仅适合小状态的情况。
         * FsStateBackend：状态存储在文件系统中，适合较大的状态，但需要额外的存储配置。
         * RocksDBStateBackend：基于 RocksDB 的状态后端，适合超大状态的处理，并支持持久化到本地文件系统或 HDFS。
         */
        StateBackend stateBackend = new RocksDBStateBackend("hdfs://hadoop102:8020/ckpt");
        env.setStateBackend(stateBackend);

        env.enableCheckpointing( 2000, CheckpointingMode.EXACTLY_ONCE);  // 传入两个最基本ck参数；间隔时长，ck模式
        CheckpointConfig checkpointConfig =env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ckpt");
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMinutes(10000)); // 设置ck对齐的超时时长
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置ck算法模式
        checkpointConfig.setCheckpointInterval(2000); // ck的间隔时长
        //checkpointConfig.setCheckpointId0fIgnoredInFlightData(5);   // 用于非对齐算法模式下，在job恢复时让各个算子自动抛弃掉ck-5中飞行数据
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // job cancel时，保留最后一次ck数据
        checkpointConfig.setForceUnalignedCheckpoints(false); // 是否强制使用 非对齐的checkpoint模式
        checkpointConfig.setMaxConcurrentCheckpoints(5); // 允许在系统中同时存在的飞行中(未完成的)的ck数
        checkpointConfig.setMinPauseBetweenCheckpoints(2000); // 设置两次ck之的最小时间间隔，用于防止checkpoint 过多地占用算子的处理时间
        checkpointConfig.setCheckpointTimeout(3000); //一个算子在一次checkpoint执行过程中的总耗费时长超时上限
        checkpointConfig.setTolerableCheckpointFailureNumber(10); // 允许的checkpoint失败最大次数
    }
}