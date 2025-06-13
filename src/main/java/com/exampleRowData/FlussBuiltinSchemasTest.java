package com.exampleRowData;


import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public class FlussBuiltinSchemasTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Test with built-in RowData schemas
        FlussSource<RowData> rowDataSource = FlussSource.<RowData>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();

        FlussSink<RowData> rowDataSink = FlussSink.<RowData>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_B")
                .setSerializationSchema(new RowDataSerializationSchema(false, false)) // upsert mode, don't ignore deletes
                .build();

        env.fromSource(rowDataSource, WatermarkStrategy.noWatermarks(), "RowData Source")
                .map(rowData -> {
                    System.out.println("ROWDATA: " + rowData);
                    return rowData;
                })
                .sinkTo(rowDataSink)
                .name("RowData Sink");

        env.execute("Fluss DataStream API - Built-in Schemas Test");
    }
}
