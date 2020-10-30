package com.izzyacademy.services;

import estreams60.ecommerce.products.Key;
import estreams60.ecommerce.products.Value;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

public class ProductTransformService implements KafkaStreamService{

    private static final String CLIENT_ID = "20";
    private static final String APP_ID = "product_enrichment_service";

    private static final String PRODUCT_TOPIC = "estreams60.ecommerce.products";
    private static final String PRODUCT_DETAILS_TOPIC = "estreams60.ecommerce.product_details";
    private static final String PRODUCT_ENRICHED_TOPIC = "estreams60.ecommerce.products_enriched";

    public ProductTransformService()
    {
        com.fasterxml.jackson.core.exc.InputCoercionException e;

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

        Map<String, String> serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final SpecificAvroSerde<Key> productKeySerde = new SpecificAvroSerde<>();
        productKeySerde.configure(serdeConfig, true);

        final SpecificAvroSerde<estreams60.ecommerce.product_details.Key> productDetailsKeySerde = new SpecificAvroSerde<>();
        productKeySerde.configure(serdeConfig, true);

        final SpecificAvroSerde<estreams60.ecommerce.product_enriched.Key> productEnrichedKeySerde = new SpecificAvroSerde<>();
        productKeySerde.configure(serdeConfig, true);

        final SpecificAvroSerde<Value> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<estreams60.ecommerce.product_details.Value> productDetailsSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<estreams60.ecommerce.product_enriched.Value> productEnrichedSerde = new SpecificAvroSerde<>();
        productEnrichedSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Key, Value> productStream = builder.stream(PRODUCT_TOPIC, Consumed.with(productKeySerde, productSerde));

        productStream.filter((k, v)-> v.getProductId() > 4001).to(PRODUCT_ENRICHED_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
