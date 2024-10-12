package com.lentra.realtime.entity.Topic;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.config.store.ConfigStoreBuilder;
import com.lentra.realtime.constants.TopicConstants;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.enums.TopicType;
import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
import com.lentra.realtime.exception.PipelineInitializationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class TopicBuilderTest {

    private static TopicBuilder sourceTopicBuilder;
    private static TopicBuilder destinationTopicBuilder;

    @BeforeAll
    public static void setupClass() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException, PipelineInitializationException {
        AppStore.store = new ConfigStoreBuilder("src/test/resources/test_mapping.properties").build();
        sourceTopicBuilder = new TopicBuilder(EndPointType.SOURCE);
        destinationTopicBuilder = new TopicBuilder(EndPointType.DESTINATION);
    }

    @Test
    public void testGetUserDefinedSourceTopicList() throws PipelineInitializationException {
        List<String> sourceTopic = sourceTopicBuilder.getUserDefinedSourceTopicList();
        assertEquals(Arrays.asList("1001.flexi-mc-uat-qa.flexi-gonogo.loanCharges", "1001.flexi-mc-uat-qa.flexi-gonogo.dmDetails", "1001.4304.ldp.asset_item"), sourceTopic);
    }

    @Test
    public void testGetUserDefinedDestinationTopicList() {
        List<String> destinationTopic = destinationTopicBuilder.getUserDefinedDestinationTopicList();
        assertEquals("[target1, dummy_target_topic]", destinationTopic.toString());
    }

    @Test
    public void testGetSourceTopicNamesFromUserDefinedList() throws PipelineInitializationException {
        List<String> sourceTopicsWithoutPrefix = sourceTopicBuilder.getSourceTopicNamesFromUserDefinedList();
        assertEquals(Arrays.asList("loanCharges", "dmDetails", "asset_item"), sourceTopicsWithoutPrefix);
    }

    @Test
    public void testGetDestinationTopicNamesFromUserDefinedList() throws PipelineInitializationException {
        List<String> destinationTopicsWithoutPrefix = destinationTopicBuilder.getDestinationTopicNamesFromUserDefinedList();
        assertEquals("[target1, dummy_target_topic]", destinationTopicsWithoutPrefix.toString());
    }

    @Test
    public void testGetTopicsForSource() throws PipelineInitializationException {
        Set<Topic> topics = sourceTopicBuilder.getTopics();
        assertFalse(topics.isEmpty());
        assertTrue(topics.stream().anyMatch(t -> t.getTopicName().equals("loanCharges")));
        assertTrue(topics.stream().anyMatch(t -> t.getTopicName().equals("dmDetails")));
        assertTrue(topics.stream().anyMatch(t -> t.getTopicName().equals("asset_item")));
    }

    @Test
    public void testGetTopicsForDestination() throws PipelineInitializationException {
        Set<Topic> topics = destinationTopicBuilder.getTopics();
        assertFalse(topics.isEmpty());
        assertTrue(topics.stream().anyMatch(t -> t.getTopicName().equals("target1")));
    }

    @Test
    public void testPipelineInitializationExceptionForEmptySourceTopicList() {
        TopicBuilder emptySourceTopicBuilder = new TopicBuilder(EndPointType.SOURCE) {
            @Override
            public List<String> getSourceTopicNamesFromUserDefinedList() throws PipelineInitializationException {
                return Collections.emptyList();
            }
        };
        assertThrows(PipelineInitializationException.class, emptySourceTopicBuilder::getTopics);
    }

    @Test
    public void testPipelineInitializationExceptionForEmptyDestinationTopicList() {
        TopicBuilder emptyDestinationTopicBuilder = new TopicBuilder(EndPointType.DESTINATION) {
            @Override
            public List<String> getDestinationTopicNamesFromUserDefinedList() throws RuntimeException {
                return Collections.singletonList("");
            }
        };
        assertThrows(PipelineInitializationException.class, emptyDestinationTopicBuilder::getTopics);
    }

}


//package com.lentra.realtime.entity.Topic;
//
//import com.lentra.realtime.config.store.AppStore;
//import com.lentra.realtime.config.store.ConfigStoreBuilder;
//
//import com.lentra.realtime.entity.Field.Field;
//import com.lentra.realtime.enums.EndPointType;
//import com.lentra.realtime.enums.TopicType;
//import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;
//import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
//import com.lentra.realtime.exception.PipelineInitializationException;
//import org.junit.jupiter.api.*;
//
//import java.util.*;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class TopicBuilderTest {
//
//    EndPointType sourceEndPoint= EndPointType.SOURCE;
//    EndPointType destinationEndPoint=EndPointType.DESTINATION;
//
//    TopicBuilder sourceTopicBuilder=new TopicBuilder(sourceEndPoint);
//    TopicBuilder destinationTopicBuilder=new TopicBuilder(destinationEndPoint);
//
//    static Topic loanChargesTopic=new Topic("loanCharges");
//    static Topic dmDetailsTopic=new Topic("dmDetails");
//    static Topic asset_itemTopic=new Topic("asset_item");
//
//    static Topic target1Topic=new Topic("target1");
//
//    static Set<String> topicTypes=new HashSet<>(Arrays.asList("CDC","DE"));
//
//    @BeforeAll
//    public static void setupClass() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException, PipelineInitializationException {
//        AppStore.store = new ConfigStoreBuilder("src/test/resources/test_mapping.properties").build();
//
//        //SETTING UP SOURCE TOPICS
//
//        //dmDetails
//        dmDetailsTopic.setTopicKeyFields(new HashSet<>(Collections.singletonList("s_application_id")));
//        dmDetailsTopic.setDestinationTopic(new Topic("2002_target"));
//        dmDetailsTopic.setTopicType(TopicType.CDC);
//
//        Field s_application_id_dm=new Field("$.after._id","s_application_id","string","int",false);
//        Field ts_ms_dm=new Field("$.source.ts_ms","ts_ms",null,null,false);
//
//        dmDetailsTopic.setFieldMappings(new HashSet<>(Arrays.asList(ts_ms_dm,s_application_id_dm)));
//
//        //loanCharges
//        loanChargesTopic.setTopicKeyFields(new LinkedHashSet<>(Collections.singletonList("s_application_id")));
//        loanChargesTopic.setDestinationTopic(new Topic("1001_target"));
//        loanChargesTopic.setTopicType(TopicType.CDC);
//
//        Field s_application_id=new Field("$.after._id","s_application_id",null,null,false);
//        Field ts_ms=new Field("$.source.ts_ms","ts_ms",null,null,false);
//        Field s_api_fees=new Field("$.after.feesDetails[?(@.feeType=='API charges'||@.feeType=='API CHARGES'||@.feeType=='apicharges'||@.feeType=='apiCharges')].fee","s_api_fees","string","double",true);
//        Field s_institution_id=new Field("$.after.institutionId","s_institution_id","string","int",false);
//
//        loanChargesTopic.setFieldMappings(new LinkedHashSet<>(Arrays.asList(s_institution_id,s_api_fees,s_application_id,ts_ms)));
//
//        //asset_item
//        asset_itemTopic.setTopicKeyFields(new HashSet<>(Collections.singletonList("s_application_id")));
//        asset_itemTopic.setDestinationTopic(new Topic("1001_target"));
//        asset_itemTopic.setTopicType(TopicType.DE);
//
//        Field s_application_id_asset=new Field("$.data.app_loan_mapping[0].application_id","s_application_id",null,null,false);
//        Field s_collateral_id=new Field("$.data.id","s_collateral_id",null,null,false);
//        Field ts_ms_asset=new Field("$.created_on","ts_ms",null,null,false);
//        Field s_asset_created_datetime=new Field("$.data.asset_created_datetime","s_asset_created_datetime","string_utc_date","epoch",false);
//        Field s_date_of_lien=new Field("$.data.date_of_lien","s_date_of_lien","string_iso_date","epoch",false);
//
//        asset_itemTopic.setFieldMappings(new LinkedHashSet<>(Arrays.asList(ts_ms_asset,s_application_id_asset,s_date_of_lien,s_collateral_id,s_asset_created_datetime)));
//
//        //------------------------------------------------------------------------------------------------------------
//
//        //SETTING UP DESTINATION TOPICS
//
//        //target1
//        target1Topic.setTopicKeyFields(new HashSet<>(Collections.singletonList("s_application_id")));
//        target1Topic.setDestinationTopic(new Topic("1001_target"));
//        target1Topic.setTopicType(TopicType.CDC);
//
//        target1Topic.setFieldMappings(new HashSet<>());
//    }
//
//    @Test
//    public void shouldGiveCompleteSourceTopicNames() throws PipelineInitializationException {
//        List<String> sourceTopic=sourceTopicBuilder.getUserDefinedSourceTopicList();
//        if(!sourceTopic.get(0).isEmpty() || sourceTopic.size()>1){
//            assertEquals(new ArrayList<>(Arrays.asList("1001.flexi-mc-uat-qa.flexi-gonogo.loanCharges","1001.flexi-mc-uat-qa.flexi-gonogo.dmDetails","1001.4304.ldp.asset_item")),sourceTopic);
//        }
//    }
//
//    @Test
//    public void emptyPropertyReturnsEmptySourceTopicList() throws PipelineInitializationException {
//        List<String> sourceTopic=sourceTopicBuilder.getUserDefinedSourceTopicList();
//        if(sourceTopic.size()==1 && sourceTopic.get(0).isEmpty()){
//            assertEquals(new ArrayList<>(Collections.singletonList("")),sourceTopic);
//        }
//    }
//
////    @Test
////    public void shouldGiveCompleteDestinationTopicNames() {
////        List<String> destinationTopic=destinationTopicBuilder.getUserDefinedDestinationTopicList();
////        if(!destinationTopic.get(0).isEmpty() || destinationTopic.size()>1){
////            assertEquals(new ArrayList<>(Collections.singletonList("target1")),destinationTopic);
////        }
////    }
//    @Test
//    public void shouldGiveCompleteDestinationTopicNames() {
//        List<String> destinationTopic=destinationTopicBuilder.getUserDefinedDestinationTopicList();
//
//        assertEquals("[target1, dummy_target_topic]",destinationTopic.toString());
//
//    }
//
//    @Test
//    public void emptyPropertyReturnsEmptyDestinationTopicList(){
//        List<String> destinationTopic=destinationTopicBuilder.getUserDefinedDestinationTopicList();
//        if(destinationTopic.size()==1 && destinationTopic.get(0).isEmpty()){
//            assertEquals(new ArrayList<>(Collections.singletonList("")),destinationTopic);
//        }
//    }
//
//    @Test
//    public void shouldGiveSourceTopicNamesWithoutPrefix() throws PipelineInitializationException {
//        List<String> sourceTopicsWithoutPrefix=sourceTopicBuilder.getSourceTopicNamesFromUserDefinedList();
//        if(!sourceTopicsWithoutPrefix.get(0).isEmpty() || sourceTopicsWithoutPrefix.size()>1){
//            assertEquals(new ArrayList<>(Arrays.asList("loanCharges", "dmDetails","asset_item")), sourceTopicsWithoutPrefix);
//        }
//    }
//
//    @Test
//    public void emptyPropertyReturnsEmptySourceTopicsWithoutPrefix() throws PipelineInitializationException {
//        List<String> sourceTopicsWithoutPrefix=sourceTopicBuilder.getSourceTopicNamesFromUserDefinedList();
//        if(sourceTopicsWithoutPrefix.size()==1 && sourceTopicsWithoutPrefix.get(0).isEmpty()) {
//            assertEquals(new ArrayList<>(Collections.singletonList("")), sourceTopicsWithoutPrefix);
//        }
//    }
//
//    @Test
//    public void shouldGiveDestinationTopicNamesWithoutPrefix() throws PipelineInitializationException {
//        List<String> destinationTopicsWithoutPrefix=destinationTopicBuilder.getDestinationTopicNamesFromUserDefinedList();
//        if(!destinationTopicsWithoutPrefix.get(0).isEmpty() || destinationTopicsWithoutPrefix.size()>1){
//            assertEquals("[target1, dummy_target_topic]", destinationTopicsWithoutPrefix.toString());
//        }
//    }
//
//    @Test
//    public void emptyPropertyReturnsEmptyDestinationTopicsWithoutPrefix() throws PipelineInitializationException {
//        List<String> destinationTopicsWithoutPrefix=destinationTopicBuilder.getDestinationTopicNamesFromUserDefinedList();
//        if(destinationTopicsWithoutPrefix.size()==1 && destinationTopicsWithoutPrefix.get(0).isEmpty()) {
//            assertEquals(new ArrayList<>(Collections.singletonList("")), destinationTopicsWithoutPrefix);
//        }
//    }
//
//    @Test
//    public void checkForSourceTopicLevelDetailsExceptFields() throws PipelineInitializationException {
//        Set<Topic> actualTopics=sourceTopicBuilder.getTopics();
//
//        // Extract loanCharges details from actualTopics
//        Optional<Topic> loanChargesInfo = getTopicDetails(actualTopics,"loanCharges");
//        Optional<Topic> dmDetailsInfo = getTopicDetails(actualTopics,"dmDetails");
//        Optional<Topic> asset_itemInfo = getTopicDetails(actualTopics,"asset_item");
//
//        //Check for event type
//        assertTrue(topicTypes.contains(loanChargesInfo.get().getTopicType().toString()),"Checking the event type for loanCharges");
//        assertTrue(topicTypes.contains(dmDetailsInfo.get().getTopicType().toString()), "Checking the event type for dmDetails");
//        assertTrue(topicTypes.contains(asset_itemInfo.get().getTopicType().toString()),"Checking the event type for asset_item");
//
//        //Check for key field
//        assertEquals(loanChargesInfo.get().getTopicKeyFields(),loanChargesTopic.getTopicKeyFields(), "Checking the key fields for loanCharges");
//        assertEquals(dmDetailsInfo.get().getTopicKeyFields(),dmDetailsTopic.getTopicKeyFields(), "Checking the key fields for dmDetails");
//        assertEquals(asset_itemInfo.get().getTopicKeyFields(),asset_itemTopic.getTopicKeyFields(), "Checking the key fields for asset_item");
//
//        //Check for destination topic
//        assertEquals(loanChargesInfo.get().getDestinationTopic().getTopicName(),loanChargesTopic.getDestinationTopic().getTopicName(),"Checking the destination topic for loanCharges");
//        assertEquals(dmDetailsInfo.get().getDestinationTopic().getTopicName(),dmDetailsTopic.getDestinationTopic().getTopicName(),"Checking the destination topic for dmDetails");
//        assertEquals(loanChargesInfo.get().getDestinationTopic().getTopicName(),loanChargesTopic.getDestinationTopic().getTopicName(),"Checking the destination topic for loanCharges");
//    }
//
//    @Test
//    public void checkForSourceTopicFieldsForAllTopics() throws PipelineInitializationException {
//        Set<Topic> actualTopics=sourceTopicBuilder.getTopics();
//
//        // Extract loanCharges details from actualTopics
//        Optional<Topic> loanChargesInfo = getTopicDetails(actualTopics,"loanCharges");
//        Optional<Topic> dmDetailsInfo = getTopicDetails(actualTopics,"dmDetails");
//        Optional<Topic> asset_itemInfo = getTopicDetails(actualTopics,"asset_item");
//
//        //Checking loanCharges Fields
//        for(Field field : loanChargesInfo.get().getFieldMappings()){
//            for(Field manualField : loanChargesTopic.getFieldMappings()){
//                if(field.getPiontFieldName().equals(manualField.getPiontFieldName())){
//                    assertEquals(manualField.toString(),field.toString(),"loanCharges");
//                    break;
//                }
//            }
//        }
//
//        //Checking dmDetails Fields
//        for(Field field : dmDetailsInfo.get().getFieldMappings()){
//            for(Field manualField : dmDetailsTopic.getFieldMappings()){
//                if(field.getPiontFieldName().equals(manualField.getPiontFieldName())){
//                    assertEquals(manualField.toString(),field.toString(),"dmDetails");
//                    break;
//                }
//            }
//        }
//
//        //checking asset_item Fields
//        for(Field field : asset_itemInfo.get().getFieldMappings()){
//            for(Field manualField : asset_itemTopic.getFieldMappings()){
//                if(field.getPiontFieldName().equals(manualField.getPiontFieldName())){
//                    assertEquals(manualField.toString(),field.toString(),"asset_item");
//                    break;
//                }
//            }
//        }
//    }
//
//    @Test
//    public void checkForDestinationTopicLevelDetailsExceptFields() throws PipelineInitializationException {
//        Set<Topic> actualTopics=destinationTopicBuilder.getTopics();
//
//        // Extract loanCharges details from actualTopics
//        Optional<Topic> target1Info = getTopicDetails(actualTopics,"target1");
//
//        //Check for event type
//        assertEquals(target1Info.get().getTopicType(),target1Topic.getTopicType(),"Checking the event type for loanCharges");
//
//        //Check for key field
//        assertEquals(target1Info.get().getTopicKeyFields(),target1Topic.getTopicKeyFields(), "Checking the key fields for loanCharges");
//
//        //Check for destination topic
//        assertEquals(target1Info.get().getDestinationTopic().getTopicName(),target1Topic.getDestinationTopic().getTopicName(),"Checking the destination topic for loanCharges");
//    }
//
//    @Test
//    public void checkForDestinationTopicFieldsForAllTopics() throws PipelineInitializationException {
//        Set<Topic> actualTopics=destinationTopicBuilder.getTopics();
//
//        // Extract loanCharges details from actualTopics
//        Optional<Topic> target1Info = getTopicDetails(actualTopics,"target1");
//
//        //Checking loanCharges Fields
//        for(Field field : target1Info.get().getFieldMappings()){
//            for(Field manualField : target1Topic.getFieldMappings()){
//                if(field.getPiontFieldName().equals(manualField.getPiontFieldName())){
//                    assertEquals(manualField.toString(),field.toString(),"target1");
//                    break;
//                }
//            }
//        }
//    }
//
////    @Test
////    public void testWhereNoDetailsAreGiven(){
////        assertThrows(PipelineInitializationException.class,() -> );
////    }
//
//    public Optional<Topic> getTopicDetails(Set<Topic> allTopics, String specificTopicName){
//        return allTopics.stream()
//                .filter(topic -> specificTopicName.equals(topic.getTopicName()))
//                .findFirst();
//    }
//}
