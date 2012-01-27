package org.pingles.cascading.protobuf;

import org.junit.Test;

import static org.junit.Assert.*;

public class PersonTest {
    @Test
    public void shouldConstructPerson() {
        assertNotNull(personBuilder().setEmail("test@pingles.org").setId(1).setName("Paul").build());
    }

    @Test
    public void shouldLetPeopleHaveFriends() {
        Messages.Person strongbad = personBuilder().setEmail("sbemail@homestarrunner.com").setId(2).setName("Strongbad").build();
        Messages.Person paul = personBuilder().setEmail("test@pingles.org").setId(1).setName("Paul").addFriends(strongbad).build();

        assertEquals(1, paul.getFriendsCount());
        assertEquals("sbemail@homestarrunner.com", paul.getFriends(0).getEmail());
    }

    private Messages.Person.Builder personBuilder() {
        return Messages.Person.newBuilder();
    }
}