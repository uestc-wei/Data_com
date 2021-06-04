package com.wei.feature.getHotItem;


import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

import com.wei.pojo.UserBehavior;
import com.wei.util.DataSourceFactory;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 统计热门topN 商品 sql版本
 */
public class GetHotItemSql {

    public static void main(String[] args) throws Exception {

        //kafkaString source
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStreamSource<String> source = new DataSourceFactory(parameterTool).
                kafkaStringSourceProduce();

        //转换成pojo，分配时间戳和watermark
        source.map(line->{
            String[] split = line.split(",");
            return new UserBehavior(new Long(split[0]),new Long(split[1]),new Integer(split[2]),
                    split[3],new Long(split[4]));
        }).assignTimestampsAndWatermarks(new Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {//设置时间延迟
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp()*1000L;
                    }
                }
        ));

        //创建表执行环境
        EnvironmentSettings tableBuildSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment
                .create(source.getExecutionEnvironment(), tableBuildSetting);
        //1.将流转成表 tableApi
        Table dataTable = tableEnv.fromDataStream(source,$("userId"),$("itemId"),
                $("categoryId"),$("behavior"),$("timestamp").rowtime().as("ts"));

        //分组开窗
        Table windowAggTable = dataTable.filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("ts")).as("w"))
                .groupBy($("itemId"), $("w"))
                .select($("itemId"), $("W").end().as("windowEnd"), $("itemId").count().as("cnt"));
        //计算top n
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg",aggStream,
                $("itemId"),$("windowEnd"),$("cnt"));

        Table resultTable = tableEnv.sqlQuery("select * from"
                + " (select *,ROW_NUMBER() over (partition by windowEnd order by cnt desc)"
                + " as row_num from agg) where row_num <=5");
        //2.sql版本
        /*tableEnv.createTemporaryView("data_table",source,$("userId"),$("itemId"),
                $("categoryId"),$("behavior"),$("timestamp").rowtime().as("ts"));
        Table sqlResultTable = tableEnv.sqlQuery("select * from"
                + " (select *,ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num"
                + " from (select itemId,count(itemId) as cnt, HOP_END(ts,interval '5' minute,interval '1' hour) as windowEnd"
                + " from data_table "
                + " where behavior = 'pv'"
                + " group by itemId,HOP(ts,interval '5' minute,interval '1' hour)"
                + " )"
                + ")"
                + "where row_num <=5 ");
        tableEnv.toRetractStream(sqlResultTable,Row.class).print();*/
        tableEnv.toRetractStream(resultTable,Row.class).print();

        source.getExecutionEnvironment().execute("hot-sql");

    }
}
