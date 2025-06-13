package com.scanModes;

import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.exampleUpsert.PersonPKDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussEarliestModeTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Test OffsetsInitializer.earliest() - All changelogs from beginning
        FlussSource<Person> earliestSource = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.earliest()) // All changelogs
                .setDeserializationSchema(new PersonPKDeserializationSchema())
                .build();

        env.fromSource(earliestSource, WatermarkStrategy.noWatermarks(), "Earliest Mode Test")
                .map(person -> {
                    System.out.println("EARLIEST-MODE: " + person);
                    return person;
                })
                .print("EARLIEST-MODE-OUTPUT");

        env.execute("Fluss DataStream API - Earliest Mode Test");
    }
}