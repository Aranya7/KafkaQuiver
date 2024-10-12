package com.lentra.realtime.entity.Topic;

import com.lentra.realtime.enums.TopicType;
import com.lentra.realtime.exception.PipelineInitializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TopicTest {

    private Topic topic;

    @BeforeEach
    void setUp() {
        topic = new Topic("testTopic");
    }

    @Test
    void testTopicCreation() {
        assertEquals("testTopic", topic.getTopicName());
        assertNotNull(topic.getDestinationTopic());
    }

    @Test
    void testSetAndGetTopicType() throws PipelineInitializationException {
        topic.setTopicType(TopicType.CDC);
        assertEquals(TopicType.CDC, topic.getTopicType());
    }

    @Test
    void testSetAndGetTopicKeyFields() throws PipelineInitializationException {
        List<String> keyFields = Arrays.asList("field1", "field2");
        topic.setTopicKeyFields(keyFields);
        assertEquals(new HashSet<>(keyFields), topic.getTopicKeyFields());
    }

    @Test
    void testSetTopicKeyFieldsWithEmptyList() {
        List<String> emptyList = Collections.singletonList("");
        assertThrows(PipelineInitializationException.class, () -> topic.setTopicKeyFields(emptyList));
    }

    @Test
    void testToString() throws PipelineInitializationException {
        topic.setTopicType(TopicType.CDC);
        topic.setTopicKeyFields(Collections.singletonList("key1"));
        topic.getDestinationTopic().setTopicName("destTopic");

        String expected = "Topic Name = testTopic\n" +
                "Topic Key Fields List = [key1]\n" +
                "Topic Destination Topics = destTopic\n" +
                "Topic Type = CDC\n" +
                "Topic Mapping Fields = []\n";

        assertEquals(expected, topic.toString());
    }
}