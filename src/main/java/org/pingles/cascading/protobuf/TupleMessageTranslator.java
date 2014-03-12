package org.pingles.cascading.protobuf;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

public class TupleMessageTranslator {
    private final List<Descriptors.FieldDescriptor> fields;
    private final Message message;

    public TupleMessageTranslator(Fields sourceFields, Message message) {
        this.message = message;
        this.fields = fieldsToRead(message, sourceFields);
    }

    /**
     * Translates values from Message attributes and onto the Tuple
     * @param tuple An initialised Tuple
     */
    public void translateOntoTuple(Tuple tuple) {
        if (message.isInitialized()) {
            TupleValueTranslator valueTranslator = new TupleValueTranslator(message);

            for (Descriptors.FieldDescriptor field : fields) {
                tuple.add(valueTranslator.translate(field));
            }
        }
    }

    public Tuple translate() {
        Tuple t = new Tuple();
        translateOntoTuple(t);
        return t;
    }

    private List<Descriptors.FieldDescriptor> fieldsToRead(Message message, Fields sourceFields) {
        List<Descriptors.FieldDescriptor> fields = new ArrayList<Descriptors.FieldDescriptor>();

        if (sourceFields.size() == 0) {
            for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
                fields.add(field);
            }
        } else {
            for (Comparable fieldName : sourceFields) {
                for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
                    if (field.getName().equals(fieldName)) {
                        fields.add(field);
                    }
                }
            }
        }

        return fields;
    }
}
