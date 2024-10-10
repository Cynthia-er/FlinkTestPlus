import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class flinkCDCToMysql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                        .port(3306)
                                .username("root")
                                        .password("123456")
                                                .databaseList("flinkTest")
                                                        .tableList("test.t1")
                                                                .build();
        //用cdc连接器实时获取到MySQL中变更的数据
        tenv.executeSql("CREATE TABLE t1 ("
                        +                     "   id int primary key,                 "
                        +                     "   gender string                       "+
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'flinkTest'," +
                "'table-name' = 't1'" +
                ")");

        tenv.executeSql("select * from t1").print();
        tenv.executeSql("CREATE TABLE tt2 (\n"
                +                     "   id int,                 "
                +                     "   name string,                        "
                +                     "   PRIMARY KEY(id) NOT ENFORCED                        "+
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'database-name' = 'flinkTest',\n" +
                "'hostname' = 'hadoop102',\n" +
                "'password' = '123456',\n" +
                "'port' = '3306',\n" +
                "'table-name' = 't2',\n" +
                "'username' = 'root'\n" +
                ")");

                tenv.executeSql("select * from tt2").print();
        tenv.executeSql("CREATE TABLE t3 (\n"
                +                     "   id int primary key,                 "
                +                     "   score int                       "+
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'hadoop102',\n" +
                "'port' = '3306',\n" +
                "'username' = 'root',\n" +
                "'password' = '123456',\n" +
                "'database-name' = 'flinkTest',\n" +
                "'table-name' = 't3'\n" +
                ")");

        //对获取到的数据进行处理，写回到MySQL中
        tenv.executeSql("create table t_mysql                             "
                +                     "(                                      "
                +                     "   id int primary key,                 "
                +                     "   name string,                        "
                +                     "   gender string,                       "
                +                     "   score int,                       "
                +                     "   rn bigint                       "
                +                     ")                                      "
                + "with (                                                     "
                + "      'connector' = 'jdbc',                                "
                + "      'url' = 'jdbc:mysql://hadoop102:3306/flinkTest',     "
                + "      'table-name' = 't4',                                "
                + "      'username' = 'root',                                "
                + "      'password' = '123456'                                "
                + ")                                                          "
        );

        //将表中数据插入到MySQL
        tenv.executeSql("insert into t_mysql " +
                "select * from\n" +
                "             (select t1.id,name,gender,score,row_number() over (partition by gender order by score desc) rn\n" +
                "              from t1 inner join t2 on t1.id=t2.id\n" +
                "                      inner join t3 on t2.id=t3.id) as t\n" +
                "where rn<=3");

        env.execute();
    }
}
