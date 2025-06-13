package com.exampleDelete;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.exampleUpsert.PersonPKDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussDeleteTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Add restart strategy to handle connection issues
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies
                        .fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10))
        );

        // Create source to read from PK table
        FlussSource<Person> source = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.earliest()) // Changed to latest to avoid reading all history
                .setDeserializationSchema(new PersonPKDeserializationSchema())
                .build();

        // Create sink for DELETE operations
        FlussSink<Person> deleteSink = FlussSink.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A") // Delete from same table
                .setSerializationSchema(new PersonDeleteSerializationSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Delete Test Source")
                .filter(person -> person.score != null && person.score < 87.0) // Delete low scores
                .map(person -> {
                    System.out.println("DELETING: " + person);
                    return person;
                })
                .sinkTo(deleteSink)
                .name("Delete Sink");

        env.execute("Fluss DataStream API - DELETE Test");
    }
}