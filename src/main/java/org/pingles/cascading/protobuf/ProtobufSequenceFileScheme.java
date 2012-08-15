package org.pingles.cascading.protobuf;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ProtobufSequenceFileScheme extends
		Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = Logger
			.getLogger(ProtobufSequenceFileScheme.class);
	private Descriptors.Descriptor descriptor;
	private String messageClassName;
	
	public <T extends Message> ProtobufSequenceFileScheme(Class<T> messageClass, Fields sourceFields) {
		super(sourceFields);
		this.messageClassName = messageClass.getName();
	}	  

	@Override public void sourcePrepare(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) 
	{
		Object[] pair = new Object[]{sourceCall.getInput().createKey(), 
									 sourceCall.getInput().createValue()};
		sourceCall.setContext( pair );
	}
	
	@Override
	public boolean source(FlowProcess<JobConf> arg0,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {		
		Object key = sourceCall.getContext()[ 0 ];
		Object val = sourceCall.getContext()[ 1 ];
		
	    boolean result = sourceCall.getInput().next( key, val );
	    if( !result )
	        return false;
	    
		Tuple tuple = sourceCall.getIncomingEntry().getTuple();
		tuple.clear();
		byte[] valueBytes = toBytes((BytesWritable)val);

		try {
			DynamicMessage message = parseMessage(getMessageDescriptor(),
					valueBytes);
			List<String> fieldNames = getFieldNames();

			for (String fieldName : fieldNames) {
				Descriptors.FieldDescriptor fieldDescriptor = getFieldDescriptor(
						getMessageDescriptor(), fieldName);

				if (!fieldDescriptor.isRepeated()) {
					Object fieldValue = message.getField(fieldDescriptor);
					tuple.add(convertToTupleObject(fieldValue));
				} else {
					Tuple t = new Tuple();
					for (int i1 = 0; i1 < message
							.getRepeatedFieldCount(fieldDescriptor); i1++) {
						Object fieldValue = message.getRepeatedField(
								fieldDescriptor, i1);
						t.add(convertToTupleObject(fieldValue));
					}
					tuple.add(t);
				}

			}
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}

		return true;
	}

	private List<String> getFieldNames() {
		List<String> fieldNames = new ArrayList<String>();
		if (getSourceFields().size() == 0) {
			List<Descriptors.FieldDescriptor> fields = getMessageDescriptor()
					.getFields();
			for (Descriptors.FieldDescriptor field : fields) {
				fieldNames.add(field.getName());
			}
		} else {
			for (int i = 0; i < getSourceFields().size(); i++) {
				fieldNames.add(getSourceFields().get(i).toString());
			}
		}
		return fieldNames;
	}

	private Object convertToTupleObject(Object fieldValue) {
		if (fieldValue instanceof DynamicMessage) {
			DynamicMessage msg = (DynamicMessage) fieldValue;
			List<Descriptors.FieldDescriptor> fields = msg
					.getDescriptorForType().getFields();
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

	private Descriptors.FieldDescriptor getFieldDescriptor(
			Descriptors.Descriptor messageDescriptor, String fieldName) {
		List<Descriptors.FieldDescriptor> fields = messageDescriptor
				.getFields();
		for (Descriptors.FieldDescriptor field : fields) {
			if (field.getName().equals(fieldName)) {
				return field;
			}
		}
		throw new RuntimeException("No such field " + fieldName + " on message");
	}

	private DynamicMessage parseMessage(
			Descriptors.Descriptor messageDescriptor, byte[] valueBytes)
			throws InvalidProtocolBufferException {
		DynamicMessage.Builder builder = DynamicMessage.newBuilder(
				messageDescriptor).mergeFrom(valueBytes);
		DynamicMessage message = builder.build();
		return message;
	}

	private Descriptors.Descriptor getMessageDescriptor() {
		if (descriptor == null) {
			try {
				Class<?> messageClass = Class.forName(messageClassName);
				Method descriptorMethod = messageClass
						.getMethod("getDescriptor");
				descriptor = (Descriptors.Descriptor) descriptorMethod
						.invoke(new Object[] {});
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
	public void sink(FlowProcess<JobConf> arg0,
			SinkCall<Object[], OutputCollector> arg1) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> arg0,
			Tap<JobConf, RecordReader, OutputCollector> arg1, JobConf arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void sourceCleanup( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
	{
		sourceCall.setContext( null );
	}
	
	@Override
	public void sourceConfInit(FlowProcess<JobConf> arg0,
			Tap<JobConf, RecordReader, OutputCollector> arg1, JobConf conf) {
		conf.setInputFormat( SequenceFileInputFormat.class );
	}
}