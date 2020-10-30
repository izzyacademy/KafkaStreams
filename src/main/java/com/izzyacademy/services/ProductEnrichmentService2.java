package com.izzyacademy.services;

import com.izzyacademy.models.Product;
import com.izzyacademy.models.ProductDetails;
import com.izzyacademy.models.ProductEnriched;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

public class ProductEnrichmentService2 implements KafkaStreamService{

    private static final String CLIENT_ID = "20";
    private static final String APP_ID = "product_enrichment_service";

    private static final String PRODUCT_TOPIC = "products";
    private static final String PRODUCT_DETAILS_TOPIC = "product_details";
    private static final String PRODUCT_ENRICHED_TOPIC = "products_enriched";

    public ProductEnrichmentService2()
    {

    }

    @Override
    public void run() {

        /**
        // Pass in via ENVIRONMENT variables
        final String schemaRegistryUrl = "";
        final String bootStrapServer = "";

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, CLIENT_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        Map<String, String> serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<ProductDetails> productDetailsSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<ProductEnriched> productEnrichedSerde = new SpecificAvroSerde<>();
        productEnrichedSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, Product> productStream = builder.stream(PRODUCT_TOPIC, Consumed.with(Serdes.Integer(), productSerde));

        KTable<Integer, ProductDetails> productDetailsStream = builder.table(PRODUCT_DETAILS_TOPIC, Consumed.with(Serdes.Integer(), productDetailsSerde));

        KStream<Integer, ProductEnriched> outputStream = productStream.join(productDetailsStream,
                (productValue, productDetailValue) -> new ProductEnriched(),
                Joined.with(Serdes.Integer(), productSerde, productDetailsSerde));

        outputStream.to(PRODUCT_ENRICHED_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

         **/
    }
}
