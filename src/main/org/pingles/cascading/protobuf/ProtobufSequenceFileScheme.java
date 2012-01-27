package org.pingles.cascading.protobuf;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class ProtobufSequenceFileScheme extends Scheme {
    private static final Logger LOGGER = Logger.getLogger(ProtobufSequenceFileScheme.class);
    private Descriptors.Descriptor descriptor;
    private String messageClassName;

    public ProtobufSequenceFileScheme(Class messageClass, Fields sourceFields) {
        super(sourceFields);
        this.messageClassName = messageClass.getName();
    }

    @Override
    public void sourceInit(Tap tap, JobConf conf) throws IOException {
        conf.setInputFormat(SequenceFileInputFormat.class);
    }

    @Override
    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {
    }

    @Override
    public Tuple source(Object key, Object val) {
        Fields sourceFields = getSourceFields();
        Tuple tuple = new Tuple();

        byte[] valueBytes = toBytes((BytesWritable) val);

        try {
            DynamicMessage message = parseMessage(getMessageDescriptor(), valueBytes);
            for (int i = 0; i < sourceFields.size(); i++) {
                String fieldName = sourceFields.get(i).toString();

                Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(getMessageDescriptor(), fieldName);
                
                if (!fieldDescriptor.isRepeated()) {
                    Object fieldValue = message.getField(fieldDescriptor);
                    tuple.add(convertToTupleObject(fieldValue));
                } else {
                    Tuple t = new Tuple();
                    for (int i1 = 0; i1 < message.getRepeatedFieldCount(fieldDescriptor); i1++) {
                        Object fieldValue = message.getRepeatedField(fieldDescriptor, i1);
                        t.add(convertToTupleObject(fieldValue));
                    }
                    tuple.add(t);
                }

            }
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        return tuple;
    }

    private Object convertToTupleObject(Object fieldValue) {
        if (fieldValue instanceof  DynamicMessage) {
            DynamicMessage msg = (DynamicMessage)fieldValue;
            List<Descriptors.FieldDescriptor> fields = msg.getDescriptorForType().getFields();
            Tuple t = new Tuple();
            for (Descriptors.FieldDescriptor field : fields) {
                Object value = msg.getField(field);
                t.add(value);
            }
            return t;
        } else {
            return fieldValue;
        }
    }

    private Descriptors.FieldDescriptor getFieldDescriptor(Descriptors.Descriptor messageDescriptor, String fieldName) {
        List<Descriptors.FieldDescriptor> fields = messageDescriptor.getFields();
        for (Descriptors.FieldDescriptor field : fields) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        throw new RuntimeException("No such field " + fieldName + " on message");
    }

    private DynamicMessage parseMessage(Descriptors.Descriptor messageDescriptor, byte[] valueBytes) throws InvalidProtocolBufferException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor).mergeFrom(valueBytes);
        DynamicMessage message = builder.build();
        return message;
    }

    private Descriptors.Descriptor getMessageDescriptor() {
        if (descriptor == null) {
            try {
                Class messageClass = Class.forName(messageClassName);
                Method descriptorMethod = messageClass.getMethod("getDescriptor");
                descriptor = (Descriptors.Descriptor) descriptorMethod.invoke(new Object[]{});
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
        return descriptor;
    }

    private byte[] toBytes(BytesWritable val) {
        byte[] bytes = val.getBytes();
        byte[] valueBytes = new byte[val.getLength()];
        System.arraycopy(bytes, 0, valueBytes, 0, val.getLength());
        return valueBytes;
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
    }
}