package com.izzyacademy.services;

import estreams66.ecommerce.product_details.Value;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

public class ProductEnrichmentService implements KafkaStreamService{

    private static final String CLIENT_ID = "66";
    private static final String APP_ID = "product_enrichment_services";

    private static final String PRODUCT_TOPIC = "estreams66.ecommerce.products";
    private static final String PRODUCT_DETAILS_TOPIC = "estreams66.ecommerce.product_details";
    private static final String PRODUCT_ENRICHED2_TOPIC = "estreams66.ecommerce.products_enriched2";

    public ProductEnrichmentService()
    {

    }

    @Override
    public void run() {

        // Pass in via ENVIRONMENT variables
        final String schemaRegistryUrl = "http://schemaregistry-external:8081";
        final String bootStrapServer = "broker1.izzyacademy.com:9093,broker2.izzyacademy.com:9093,broker3.izzyacademy.com:9093,broker4.izzyacademy.com:9093,broker5.izzyacademy.com:9093";

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, CLIENT_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);


        final SpecificAvroSerde<estreams66.ecommerce.products.Value> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<estreams66.ecommerce.product_details.Value> productDetailsSerde = new SpecificAvroSerde<>();
        productDetailsSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<estreams66.ecommerce.product_enriched.Value> productEnrichedSerde = new SpecificAvroSerde<>();
        productEnrichedSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, estreams66.ecommerce.products.Value> productStream = builder.stream(PRODUCT_TOPIC, Consumed.with(Serdes.Integer(), productSerde));

        KStream<Integer, estreams66.ecommerce.product_details.Value> productDetailsTable = builder.stream(PRODUCT_DETAILS_TOPIC, Consumed.with(Serdes.Integer(), productDetailsSerde));

        KStream<Integer, estreams66.ecommerce.product_enriched.Value> outputStream = productStream.join(productDetailsTable,
          (productValue, productDetailValue) -> joiner(productValue, productDetailValue),
              JoinWindows.of(Duration.ofSeconds(5)),
            StreamJoined.with(Serdes.Integer(), productSerde, productDetailsSerde));

        outputStream.to(PRODUCT_ENRICHED2_TOPIC, Produced.with(Serdes.Integer(), productEnrichedSerde));

        outputStream.print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static estreams66.ecommerce.product_enriched.Value joiner(estreams66.ecommerce.products.Value product, estreams66.ecommerce.product_details.Value details) {

        estreams66.ecommerce.product_enriched.Value result = new estreams66.ecommerce.product_enriched.Value();

        result.setProductId(product.getProductId());
        result.setName(product.getName());
        result.setDateCreated(product.getDateCreated());
        result.setDateModified(product.getDateModified());
        result.setDepartment(product.getDepartment());

        result.setLongDescription(details.getLongDescription());

        return result;
    }
}
