package com.scanModes;

import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.exampleUpsert.PersonPKDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussFullModeTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Test OffsetsInitializer.full() - Default for PK tables
        FlussSource<Person> fullSource = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.full()) // Snapshot + all changelogs
                .setDeserializationSchema(new PersonPKDeserializationSchema())
                .build();

        env.fromSource(fullSource, WatermarkStrategy.noWatermarks(), "Full Mode Test")
                .map(person -> {
                    System.out.println("FULL-MODE: " + person);
                    return person;
                })
                .print("FULL-MODE-OUTPUT");

        env.execute("Fluss DataStream API - Full Mode Test");
    }
}