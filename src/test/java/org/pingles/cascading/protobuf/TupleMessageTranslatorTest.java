package org.pingles.cascading.protobuf;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

public class TupleMessageTranslatorTest {
    @Test
    public void shouldNotSetTupleForUninitialisedMessage() {
        Messages.Person p = Messages.Person.newBuilder().buildPartial();
        TupleMessageTranslator t = new TupleMessageTranslator(Fields.ALL, p);

        Tuple tuple = t.translate();

        assertEquals(0, tuple.size());
    }

    @Test
    public void shouldTranslateOntoTuple() {
        Messages.Person p = Messages.Person.newBuilder().setId(123).setName("Paul").build();

        TupleMessageTranslator t = new TupleMessageTranslator(Fields.ALL, p);
        Tuple tuple = t.translate();

        assertEquals(5, tuple.size());
        assertNotNull(tuple.getInteger(0));
        assertNotNull(tuple.getString(1));

        assertNull(tuple.getObject(2));
        List friends = (List) tuple.getObject(3);
        assertEquals(0, friends.size());

        assertNull(tuple.getObject(4));
    }
}
