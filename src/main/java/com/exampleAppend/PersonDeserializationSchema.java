package com.exampleAppend;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;
import com.common.Person;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class PersonDeserializationSchema implements FlussDeserializationSchema<Person> {

    @Override
    public void open(InitializationContext context) throws Exception {
        // Initialization logic if needed
    }

    @Override
    public Person deserialize(LogRecord logRecord) throws Exception {
        if (logRecord == null) {
            return null;
        }

        // Extract the internal row from the log record
        InternalRow row = logRecord.getRow();
        if (row == null) {
            return null;
        }

        // Extract fields from the Fluss internal row format
        Person person = new Person();
        person.id = row.isNullAt(0) ? null : row.getLong(0);
        person.name = row.isNullAt(1) ? null : row.getString(1).toString();
        person.age = row.isNullAt(2) ? null : row.getInt(2);
        person.score = row.isNullAt(3) ? null : row.getDouble(3);

        return person;
    }

    @Override
    public TypeInformation<Person> getProducedType(RowType rowType) {
        return TypeInformation.of(Person.class);
    }
}