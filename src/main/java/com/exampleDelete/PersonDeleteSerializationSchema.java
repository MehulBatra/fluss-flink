package com.exampleDelete;

import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.row.GenericRow;
import com.common.Person;

public class PersonDeleteSerializationSchema implements FlussSerializationSchema<Person> {

    @Override
    public void open(InitializationContext context) throws Exception {}

    @Override
    public RowWithOp serialize(Person person) throws Exception {
        if (person == null || person.id == null) {
            return null;
        }

        // IMPORTANT: Create row with SAME number of fields as your table
        // If your table has 4 fields (id, name, age, score), use 4
        // If your table has 5 fields (id, name, age, score, processed_time), use 5

        // Check your table schema first!
        // For 4-field table (without processed_time):
        GenericRow row = new GenericRow(4);
        row.setField(0, person.id);    // Primary key - required for DELETE
        row.setField(1, null);         // name - not needed for DELETE
        row.setField(2, null);         // age - not needed for DELETE
        row.setField(3, null);         // score - not needed for DELETE

//         For 5-field table (with processed_time), use this instead:
//         GenericRow row = new GenericRow(5);
//         row.setField(0, person.id);
//         row.setField(1, null);
//         row.setField(2, null);
//         row.setField(3, null);
//         row.setField(4, null);

        return new RowWithOp(row, OperationType.DELETE);
    }
}