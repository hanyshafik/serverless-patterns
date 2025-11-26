package com.amazonaws.services.lambda.samples.events.kafka;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.Producer;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Lambda function handler that produces AVRO messages to a Kafka topic
 */
public class AvroProducerHandler implements RequestHandler<Map<String, Object>, String> {

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Received event: " + gson.toJson(event));

        // Initialize counters for zip code distribution
        int messageCount = 10;
        int zip1000Count = 0;
        int zip2000Count = 0;

        try {
            // Get environment variables
            //String mskClusterArn = System.getenv("MSK_CLUSTER_ARN");
            //String kafkaTopic = System.getenv("MSK_TOPIC");
	        String bootstrapBrokers = System.getenv("BOOTSTRAP_BRssOKERS");
            String kafkaTopic = System.getenv("KAFKA_TOPIC");
            String schemaName = System.getenv("SCHEMA_NAME");
            String region = System.getenv("AWS_REGION");
            String registryName = System.getenv("REGISTRY_NAME") != null ? 
                                 System.getenv("REGISTRY_NAME") : "default-registry";

            if (bootstrapBrokers == null || kafkaTopic == null || schemaName == null) {
                throw new RuntimeException("Required environment variables not set: BOOTSTRAP_BROKERS, KAFKA_TOPIC, SCHEMA_NAME");
            }

            // Log that we're generating glucose readings
            logger.log("Generating glucose readings");
            
            // Create a GlucoseReading object from the input event or use default values
            GlucoseReading glucoseReading = createGlucoseReadingFromEvent(event);
            logger.log("Created glucoseReading: " + gson.toJson(glucoseReading));

            
            
            // Log the topic name for debugging
            logger.log("Target Kafka topic: '" + kafkaTopic + "'");
            
            // Create Kafka producer with AWS Glue Schema Registry serializer
            try (Producer<String, GlucoseReading> producer = KafkaProducerHelper.createProducer(
                    bootstrapBrokers, region, registryName, schemaName)) {
                
                // Log producer configuration
                logger.log("Created Kafka producer with AWS Glue Schema Registry serializer");
                logger.log("Registry name: " + registryName);
                logger.log("Schema name: " + schemaName);
                
                // Send 10 messages
                logger.log("Sending " + messageCount + " AVRO messages to topic: " + kafkaTopic);
                
                for (int i = 0; i < messageCount; i++) {
                    // Generate a random key for each message
                    String messageKey = UUID.randomUUID().toString();
                    
                    // Create a new GlucoseReading for each message to ensure variety
                    GlucoseReading messageGlucoseReading = createGlucoseReadingFromEvent(event);
                    
                    // Print the glucoseReading details before sending (GlucoseReading is now a SpecificRecord)
                    logger.log("Sending glucoseReading #" + (i+1) + ": " + gson.toJson(messageGlucoseReading));
                    logger.log("AVRO record #" + (i+1) + ": " + messageGlucoseReading.toString());
                    
                    // Log the zip code prefix for distribution tracking
                    /*String zipCode = messageGlucoseReading.getZip();
                    if (zipCode != null && zipCode.length() >= 4) {
                        String prefix = zipCode.substring(0, 4);
                        logger.log("GlucoseReading#" + (i+1) + " zip code prefix: " + prefix);
                        
                        // Count zip codes by prefix
                        if ("1000".equals(prefix)) {
                            zip1000Count++;
                        } else if ("2000".equals(prefix)) {
                            zip2000Count++;
                        }
                    }*/
                    
                    // Send the message (GlucoseReading is now a SpecificRecord)
                    KafkaProducerHelper.sendAvroMessage(producer, kafkaTopic, messageKey, messageGlucoseReading);
                    logger.log("Successfully sent AVRO message #" + (i+1) + " to topic: " + kafkaTopic);
                }
                
                // Log summary of zip code distribution
                //logger.log("ZIP CODE DISTRIBUTION SUMMARY:");
                //logger.log("Messages with zip code starting with 1000: " + zip1000Count);
                //logger.log("Messages with zip code starting with 2000: " + zip2000Count);
                //logger.log("Other zip code formats: " + (messageCount - zip1000Count - zip2000Count));
            }

            /*return "Successfully sent " + messageCount + " AVRO messages to Kafka topic: " + kafkaTopic + 
                   " (Zip codes: " + zip1000Count + " with prefix 1000, " + zip2000Count + " with prefix 2000)";*/
	    return "Successfully sent " + messageCount + " AVRO messages to Kafka topic: " + kafkaTopic; 
        } catch (Exception e) {
            logger.log("Error sending AVRO message: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to send AVRO message: " + e.getMessage(), e);
        }
    }

    /**
     * Create a GlucoseReading object from the input event or use default values
     * 
     * @param event Input event map
     * @return GlucoseReading object
     */
    private GlucoseReading createGlucoseReadingFromEvent(Map<String, Object> event) {
        GlucoseReading glucoseReading = new GlucoseReading();
        
        // Set fields from event if available, otherwise use default values
        glucoseReading.setPatientId(getStringValue(event, "patientId", "PatientId-" + randomSuffix()));
        glucoseReading.setDeviceId(getStringValue(event, "deviceId", "DeviceId-" + randomSuffix()));

        int min = 70;
        int max = 170;
        // Create an instance of the Random class
        Random random = new Random();
        // Generate the random number
        // The formula is: random.nextInt((max - min) + 1) + min;
        int randomNumber = random.nextInt((max - min) + 1) + min;
        glucoseReading.setGlucoseMgDL(randomNumber);

        
        return glucoseReading;
    }

    /**
     * Get a string value from the event map, or return a default value if not found
     * 
     * @param event Input event map
     * @param key Key to look for
     * @param defaultValue Default value to return if key not found
     * @return String value
     */
    private String getStringValue(Map<String, Object> event, String key, String defaultValue) {
        if (event.containsKey(key) && event.get(key) != null) {
            return event.get(key).toString();
        }
        return defaultValue;
    }

    /**
     * Generate a random suffix for default values
     * 
     * @return Random string
     */
    private String randomSuffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Generate a random digit
     * 
     * @return Random digit as string
     */
    private String randomDigit() {
        return Integer.toString((int) (Math.random() * 10));
    }

    /**
     * Generate random digits of specified length
     * 
     * @param length Number of digits to generate
     * @return Random digits as string
     */
    private String randomDigits(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(randomDigit());
        }
        return sb.toString();
    }
    
    /**
     * Get schema definition from AWS Glue Schema Registry
     * 
     * @param region AWS region
     * @param registryName Registry name
     * @param schemaName Schema name
     * @return Schema definition as a string
     */
    private String getSchemaDefinitionFromRegistry(String region, String registryName, String schemaName) {
        try {
            // Create Glue client with explicit HTTP client
            GlueClient glueClient = GlueClient.builder()
                    .httpClientBuilder(UrlConnectionHttpClient.builder())
                    .region(software.amazon.awssdk.regions.Region.of(region))
                    .build();
            
            // Get schema definition
            GetSchemaVersionRequest request = GetSchemaVersionRequest.builder()
                    .schemaId(SchemaId.builder()
                            .registryName(registryName)
                            .schemaName(schemaName)
                            .build())
                    .schemaVersionNumber(SchemaVersionNumber.builder().latestVersion(true).build())
                    .build();
            
            GetSchemaVersionResponse response = glueClient.getSchemaVersion(request);
            String schemaVersionId = response.schemaVersionId();
            String schemaDefinition = response.schemaDefinition();
            
            System.out.println("Retrieved schema version ID: " + schemaVersionId);
            System.out.println("Retrieved schema definition: " + schemaDefinition);
            
            return schemaDefinition;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get schema definition from registry: " + e.getMessage(), e);
        }
    }
}
