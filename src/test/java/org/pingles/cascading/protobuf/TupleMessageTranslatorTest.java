package org.pingles.cascading.protobuf;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

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

    @Test
    public void shouldTranslateNestedMessageToTuple() {
        Messages.Passport passport = Messages.Passport.newBuilder().setPassportNumber(999).build();
        Messages.Person p = Messages.Person.newBuilder().setPassport(passport).setId(123).setName("Paul").build();

        TupleMessageTranslator t = new TupleMessageTranslator(Fields.ALL, p);
        Tuple tuple = t.translate();

        Object passportOnTuple = tuple.get(4);
        assertThat(passportOnTuple, instanceOf(Tuple.class));
    }

    @Test
    public void shouldTranslateEnumOnNestedMessageToTuple() {
        Messages.Passport passport = Messages.Passport.newBuilder().setPassportNumber(999).setIssuer(Messages.Issuer.BRITISH).build();
        Messages.Person p = Messages.Person.newBuilder().setPassport(passport).setId(123).setName("Paul").build();

        TupleMessageTranslator t = new TupleMessageTranslator(Fields.ALL, p);
        Tuple personTuple = t.translate();

        Tuple passportTuple = (Tuple) personTuple.get(4);
        assertEquals(999, passportTuple.get(0));
        assertEquals("BRITISH", passportTuple.get(1));
    }
}
