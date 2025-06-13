package com.projection;

import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.exampleUpsert.PersonPKDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussProjectedFieldsTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String[] projectedFields = {"id", "name", "score"}; // Only 3 fields!

        PersonPKDeserializationSchema deserializer =
                new PersonPKDeserializationSchema(projectedFields);

        // Test setProjectedFields() - Only read specific columns
        FlussSource<Person> projectedSource = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setProjectedFields(projectedFields) // Only these fields, age will be null
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(deserializer)
                .build();

        env.fromSource(projectedSource, WatermarkStrategy.noWatermarks(), "Projected Fields Test")
                .map(person -> {
                    System.out.println("PROJECTED (id, name, score only): " + person);
                    // Note: person.age should be null due to projection
                    return person;
                })
                .print("PROJECTED-OUTPUT");

        env.execute("Fluss DataStream API - Projected Fields Test");
    }
}