package org.pingles.cascading.protobuf;

import static org.junit.Assert.*;

public class PersonTest {
    @org.junit.Test
    public void shouldConstructPerson() {
        Messages.Person.Builder builder = Messages.Person.newBuilder();
        builder.setEmail("test@pingles.org").setId(1).setName("Paul");
        assertNotNull(builder.build());
    }
}