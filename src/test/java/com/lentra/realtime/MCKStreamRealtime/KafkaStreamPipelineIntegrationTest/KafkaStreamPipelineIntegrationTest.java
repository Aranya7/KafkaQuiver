package com.lentra.realtime.MCKStreamRealtime.KafkaStreamPipelineIntegrationTest;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.config.store.ConfigStoreBuilder;
import com.lentra.realtime.exception.*;
import com.lentra.realtime.factories.PipelineFactory;
import com.lentra.realtime.pipelines.Pipeline;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class KafkaStreamPipelineIntegrationTest {
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.3.0");
    private static KafkaContainer kafkaSource;
    private static KafkaContainer kafkaDestination;

    @Mock
    private static Properties properties;

    private static String input_data_path;
    private static String input_data_key;
    private static String[] source_topic_list;
    private static String target_topic;
    private static int source_topic_number;

    @BeforeAll
    public static void setUpClass() throws IOException, InterruptedException {
        // Start Kafka producer container
        kafkaSource = new KafkaContainer(KAFKA_IMAGE)
                .withNetworkAliases("kafka-producer")
                .withExposedPorts(9093);
        kafkaSource.start();

        // Start Kafka consumer container
        kafkaDestination = new KafkaContainer(KAFKA_IMAGE)
                .withNetworkAliases("kafka-consumer")
                .withExposedPorts(9093);
        kafkaDestination.start();

        dataIngestionClass();
    }


    public static void dataIngestionClass() throws IOException, InterruptedException {
        properties = new Properties();
        //Replace the path below with the local path to the test mapping.properties file.
        properties.load(Files.newInputStream(Paths.get("src/test/resources/integration_test_mapping.properties")));

        target_topic=properties.getProperty("target_topic");


        source_topic_list=properties.getProperty("source.topic.list").split(",");;
        source_topic_number= Integer.parseInt(properties.getProperty("source_topic_number"));;

        input_data_key = properties.getProperty("test_input_date_key");
        input_data_path = properties.getProperty("test_input_data_path");

        for (String topic : source_topic_list){
            createTopic(topic);
        }

        produceMessage(input_data_key, input_data_path, source_topic_list[source_topic_number]);

        updateMappingProperties(kafkaSource.getBootstrapServers().toString().replace("PLAINTEXT://",""), kafkaDestination.getBootstrapServers().toString().replace("PLAINTEXT://",""));
        Thread.sleep(1000);

    }

    @AfterAll
    public static void tearDownClass() {
        kafkaSource.stop();
        kafkaDestination.stop();
    }

    public static void createTopic(String topicName) {
        // Kafka broker properties
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSource.getBootstrapServers());

        // Creating Kafka Admin Client
        try (AdminClient adminClient = AdminClient.create(properties)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            Map<String, String> configs = newTopic.configs();
            configs = configs != null ? configs : new HashMap<>(); // Instantiate configs if null
            configs.put("cleanup.policy", "compact");
            configs.put("segment.bytes", String.valueOf(104857600));
            newTopic.configs(configs);

            // Creating the topic
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (Exception e) {
            System.err.println("Error creating topic: " + e.getMessage());
            System.exit(1);
        }
    }

    public static void produceMessage(String key, String path, String topicName) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaSource.getBootstrapServers());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties)) {
            String value = new String(Files.readAllBytes(Paths.get(path)));
            producer.send(new ProducerRecord<>(topicName, key, value)).get();
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    @NotNull
    private static Consumer<String, String> getConsumer() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDestination.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        return consumer;
    }


    public static void updateMappingProperties(String sourceKafkaUrl, String destinationKafkaUrl) throws IOException {
        // Define the path to the mapping.properties file
        String propertiesFilePath = Paths.get("src", "test", "resources", "integration_test_mapping.properties").toString();
        String temp_properties = Paths.get("src", "test", "resources", "integration_temp_mapping.properties").toString();

        // Load the existing properties from the file
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(propertiesFilePath)) {
            properties.load(input);
        }

        // Update only the relevant properties
        properties.setProperty("source.bootstrap.url", sourceKafkaUrl);
        properties.setProperty("destination.bootstrap.url", destinationKafkaUrl);

        // Manually write properties back to the file without escaping colons
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(temp_properties))) {
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        }
    }

//    public static void modifyPropertiesFile(String fileName, String key1, String newValue1, String key2, String newValue2) throws IOException {
//        // Load the properties file from resources folder
//        String filePath = Paths.get("/home/aranyasinghchauhan/Downloads/Main_MCKStreamRealtime/MCKStreamRealtime/src/test/resources/", fileName).toString();
//        Properties properties = new Properties();
//
//        try (InputStream input = KafkaStreamPipelineTest.class.getClassLoader().getResourceAsStream(fileName)) {
//            if (input == null) {
//                System.out.println("Sorry, unable to find " + fileName);
//                return;
//            }
//            // Load properties from file
//            properties.load(input);
//        }
//
//        // Modify the required key
//        properties.setProperty(key1, newValue1);
//        properties.setProperty(key2, newValue2);
//
//        // Save the modified properties file back to the same location
//        try (OutputStream output = new FileOutputStream(filePath)) {
//            properties.store(output, null);
//        }
//    }
    @Test
    public void testPipeline() throws InterruptedException {
        // Pipeline Execution
        try {
            AppStore.store = new ConfigStoreBuilder("src/test/resources/integration_temp_mapping.properties").build();
        } catch (PipelineConfigurationNotFoundException | PipelineConfiguationContentNotFoundException e) {
            throw new RuntimeException(e);
        }

        PipelineFactory factory = new PipelineFactory();
        Pipeline pipeline = factory.getPipeline();

        try {
            pipeline.initialize();
        } catch (PipelineInitializationException e) {
            throw new RuntimeException(e);
        }
        try {
            pipeline.start();
        } catch (PipelineBuildException | PipelineAuthenticationException | EndPointConfigurationNotFound |
                 KafkaTopicNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (PropertyObservableFileComparisonException e) {
            throw new RuntimeException(e);
        }
        Thread.sleep(2000);

        //Data Validation
        Consumer<String, String> consumer = getConsumer();

//        String topics = consumer.listTopics().toString();
//        System.out.println(topics);

        consumer.subscribe(Collections.singletonList(target_topic));

        // Poll for records
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

        // Assertion: Check if records are received on destination broker.
        assertTrue(!records.isEmpty(), "No records received from Kafka topic");


        if (records.isEmpty()) {
            System.out.println("No records found");
        } else {
            for (ConsumerRecord<String, String> record : records) {
//                if record.key()
                System.out.print("Key: " + record.key());
                System.out.print("\nValue: " + record.value());
                assertTrue(record.value().equals("{\"s_api_fees\":81,\"s_application_id\":\"R-PPL23121015745\",\"ts_ms\":1704887704000,\"s_institution_id\":1001}") || record.value().equals("{\"s_api_fees\":81,\"s_application_id\":\"R-PPL23121015745\",\"s_institution_id\":1001,\"ts_ms\":1704887704000}"));
                System.out.println();
            }
        }
        // Clean up
        consumer.close();
    }
}
