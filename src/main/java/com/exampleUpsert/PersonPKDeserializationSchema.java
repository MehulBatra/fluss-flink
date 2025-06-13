package com.exampleUpsert;



import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;
import com.common.Person;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class PersonPKDeserializationSchema implements FlussDeserializationSchema<Person> {

    private String[] projectedFields;
    private String[] allFields = {"id", "name", "age", "score"};

    public PersonPKDeserializationSchema(String[] projectedFields) {
        this.projectedFields = projectedFields != null ? projectedFields : allFields;
    }

    public PersonPKDeserializationSchema() {
        this.projectedFields = allFields;
    }

    @Override
    public void open(InitializationContext initializationContext) throws Exception {
        // Initialize any resources if needed
    }

    @Override
    public Person deserialize(LogRecord logRecord) throws Exception {
        if (logRecord == null) {
            return null;
        }

        InternalRow row = logRecord.getRow();
        if (row == null) {
            return null;
        }

        Person person = new Person();

        // Map each projected field to its value based on position in projection
        for (int i = 0; i < projectedFields.length && i < row.getFieldCount(); i++) {
            if (!row.isNullAt(i)) {
                String fieldName = projectedFields[i];
                setPersonField(person, fieldName, row, i);
            }
        }

        return person;
    }

    private void setPersonField(Person person, String fieldName, InternalRow row, int index) {
        switch (fieldName) {
            case "id":
                person.id = row.getLong(index);
                break;
            case "name":
                person.name = row.getString(index).toString();
                break;
            case "age":
                person.age = row.getInt(index);
                break;
            case "score":
                person.score = row.getDouble(index);
                break;
            default:
                System.out.println("Warning: Unknown field in projection: " + fieldName);
                break;
        }
    }

    @Override
    public TypeInformation<Person> getProducedType(RowType rowType) {
        return TypeInformation.of(Person.class);
    }
}