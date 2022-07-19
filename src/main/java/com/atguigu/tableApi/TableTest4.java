package com.atguigu.tableApi;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.hash.HashCode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.AggregatedTable;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
/**
 * @ClassName TableTest4
 * @Description
 * @Author SDY
 * @Date 2022/3/20 18:17
 **/
public class TableTest4 {
    public static void main(String[] args) throws Exception {
        /**
         * 创建执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读入文件数据，得到dataStream
        DataStreamSource<String> data = env.readTextFile("D:\\study\\code\\bigdata-study-test\\src\\main\\resources\\sensor.txt");

        SingleOutputStreamOperator<SenorReading> data2 = data.map(line -> {
            String[] split = line.split(",");
            return new SenorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        // 将流转换成表，定义时间特性
        Table table = tableEnv.fromDataStream(data2, "id,timestamp as ts,temperature as tmp");

        // 使用UDF函数00
        testHashCode testHashCode = new testHashCode(23);
        testSplit testSplit = new testSplit("_");
        testAggregate testAggregate = new testAggregate();

        // 需要在环境中注册udf
        tableEnv.registerFunction("hashCode",testHashCode);
        tableEnv.registerFunction("split",testSplit);
        tableEnv.registerFunction("aggregate",testAggregate);

        Table resultTable = table
                .joinLateral("split(id) as (word,length)")
                        .select("id,ts,word,length");

        Table table3 = table.groupBy("id").aggregate("aggregate(tmp) as tmp").select("id,tmp");

        tableEnv.createTemporaryView("sensor",table);

        Table resultTable2 = tableEnv.sqlQuery("select id,ts,hashCode(id) as t from sensor");

        tableEnv.toAppendStream(resultTable,Row.class).print("result");
        tableEnv.toAppendStream(resultTable2,Row.class).print("sql");
        tableEnv.toRetractStream(table3,Row.class).print("agg");

        env.execute();


    }

    /**
     * 实现自定义的ScalarFunction111111111
     */
    public static class testHashCode extends ScalarFunction{
        private int factor = 13;

        public testHashCode(int factor){
            this.factor = factor;
        }

        public int eval(String str){
            return str.hashCode();
        }

    }


    /**
     * 实现自定义的tableFunction
     */
    public static class testSplit extends TableFunction<Tuple2<String,Integer>>{
        private String separator = "";
        // 获得分隔符
        public testSplit(String f) {this.separator = f;}

        public void eval(String str){
            for (String s : str.split(separator)){
                collect(new Tuple2<>(s,s.length()));
            }
        }
    }

    /**
     * 自定义聚合函数:求温度平均值
     */
    public static class testAggregate extends AggregateFunction<Double,Tuple2<Double,Integer>>{

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        // 必须手写一个改变累加器状态的方法,名字必须是accumulate，且无返回值，参数第一个是累加数，第二个参数是输入的参数

        public void accumulate(Tuple2<Double, Integer> acc,Double s){
            acc.f0 += s;
            acc.f1 += 1;
        }
    }
}
