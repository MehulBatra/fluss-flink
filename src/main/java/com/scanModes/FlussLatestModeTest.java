package com.scanModes;

import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.exampleUpsert.PersonPKDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class FlussLatestModeTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Test OffsetsInitializer.latest() - Only new changes
        FlussSource<Person> latestSource = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.latest()) // Only new changelogs
                .setDeserializationSchema(new PersonPKDeserializationSchema())
                .build();

        env.fromSource(latestSource, WatermarkStrategy.noWatermarks(), "Latest Mode Test")
                .map(person -> {
                    System.out.println("LATEST-MODE: " + person);
                    return person;
                })
                .print("LATEST-MODE-OUTPUT");

        env.execute("Fluss DataStream API - Latest Mode Test");
    }
}