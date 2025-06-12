package com.example;

import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.common.Person;

public class PersonPKSerializationSchema implements FlussSerializationSchema<Person> {

    @Override
    public void open(InitializationContext context) throws Exception {
        // Initialization logic if needed
    }

    @Override
    public RowWithOp serialize(Person person) throws Exception {
        if (person == null) {
            return null;
        }

        // Create a GenericRow with 5 fields (id, name, age, score, processed_time)
        GenericRow row = new GenericRow(5);
        row.setField(0, person.id);
        // Convert String to BinaryString for Fluss
        row.setField(1, person.name != null ? BinaryString.fromString(person.name) : null);
        row.setField(2, person.age);
        row.setField(3, person.score);
        row.setField(4, person.processedTime);

        // For PrimaryKey tables, use UPSERT operation (supports INSERT/UPDATE)
        return new RowWithOp(row, OperationType.UPSERT);
    }
}