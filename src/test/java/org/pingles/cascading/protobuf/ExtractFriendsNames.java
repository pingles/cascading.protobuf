package org.pingles.cascading.protobuf;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Collection;

public class ExtractFriendsNames extends BaseOperation implements Function {
    private final Fields nestedTupleFields;
    private final Fields extractFields;

    ExtractFriendsNames(Fields nestedTupleFields, Fields extractFields) {
        this.nestedTupleFields = nestedTupleFields;
        this.extractFields = extractFields;
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple friends = (Tuple) arguments.getObject("friends");

        for (Object friend : friends) {
            Tuple t = (Tuple) friend;
            TupleEntry te = new TupleEntry(nestedTupleFields, t);

            Tuple output = te.selectTuple(extractFields);
            functionCall.getOutputCollector().add( output );
        }
    }
}
