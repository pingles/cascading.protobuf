package org.pingles.cascading.protobuf;

import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class TupleValueTranslatorTest {
    @Test
    public void shouldLeavePrimitives() throws Descriptors.DescriptorValidationException {
        Messages.Person p = Messages.Person.newBuilder().setId(1234).setName("Paul").build();

        TupleValueTranslator t = new TupleValueTranslator(p);

        assertEquals("Paul", t.translate(fieldDescriptor("name")));
        assertEquals(1234, t.translate(fieldDescriptor("id")));
    }

    @Test
    public void shouldConvertMessageToTuple() {
        Messages.Person peter = Messages.Person.newBuilder().setId(999).setName("Peter").build();
        Messages.Person paul = Messages.Person.newBuilder().setId(1234).setName("Paul").addFriends(peter).build();

        TupleValueTranslator t = new TupleValueTranslator(paul);

        List friends = (List)t.translate(fieldDescriptor("friends"));
        Object firstFriend = friends.get(0);

        assertThat(firstFriend, instanceOf(Tuple.class));
        assertEquals("Peter", ((Tuple)firstFriend).getString(1));
    }

    @Test
    public void shouldTranslateUnsetFieldToNull() {
        Messages.Person p = Messages.Person.newBuilder().setId(1234).setName("Paul").build();

        TupleValueTranslator t = new TupleValueTranslator(p);

        assertNull(t.translate(fieldDescriptor("email")));
    }

    private Descriptors.FieldDescriptor fieldDescriptor(String fieldName) {
        return Messages.Person.getDescriptor().findFieldByName(fieldName);
    }
}
