package com.recoveryModes;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.exampleAppend.PersonDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussCheckpointRecoveryTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Configure checkpointing for state management
        env.enableCheckpointing(10000); // Checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        FlussSource<Person> source = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new PersonDeserializationSchema())
                .build();

        FlussSink<Person> sink = FlussSink.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_B")
                .setSerializationSchema(new com.example.PersonPKSerializationSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Checkpoint Test Source")
                .uid("fluss-source") // Important: set UIDs for state recovery
                .map(person -> {
                    person.processedTime = System.currentTimeMillis();
                    person.score = person.score != null ? person.score + 0.1 : null;
                    System.out.println("CHECKPOINT-TEST: " + person);
                    return person;
                })
                .uid("map-operator") // Important: set UIDs for state recovery
                .sinkTo(sink)
                .uid("fluss-sink") // Important: set UIDs for state recovery
                .name("Checkpoint Test Sink");

        env.execute("Fluss DataStream API - Checkpoint/Recovery Test");
    }
}