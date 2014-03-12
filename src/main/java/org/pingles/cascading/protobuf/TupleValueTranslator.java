package org.pingles.cascading.protobuf;

import cascading.tuple.Fields;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

// Helps translate from Message attributes to values suitable for storing in Tuples
public class TupleValueTranslator {
    private final Message message;

    public TupleValueTranslator(Message message) {
        this.message = message;
    }

    /**
     * Translates the value of a Message's Field to a value suitable for storing in a Tuple.
     * @param field
     * @return
     */
    public Object translate(Descriptors.FieldDescriptor field) {
        if (!field.isRepeated()) {
            if (!message.hasField(field)) {
                return null;
            }
            Object value = message.getField(field);
            return coerceValue(value);
        }

        List<Object> repeatedTuples = new ArrayList<Object>();

        for (int idx = 0; idx < message.getRepeatedFieldCount(field); idx++) {
            Object val = message.getRepeatedField(field, idx);
            repeatedTuples.add(coerceValue(val));
        }

        return repeatedTuples;
    }

    private Object coerceValue(Object o) {
        if (o instanceof Message) {
            Message m = (Message)o;

            TupleMessageTranslator messageTranslator = new TupleMessageTranslator(Fields.ALL, m);
            return messageTranslator.translate();
        }

        return o;
    }
}
