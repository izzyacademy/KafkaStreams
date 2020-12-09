package com.izzyacademy.services;


import estreams64.ecommerce.product_details.Value;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

public class ProductEnrichmentTable2TableService implements StreamMicroService {

    private static final String CLIENT_ID = "64";
    private static final String APP_ID = "product_enrichment_services";

    private static final String PRODUCT_TOPIC = "estreams64.ecommerce.products";
    private static final String PRODUCT_DETAILS_TOPIC = "estreams64.ecommerce.product_details";
    private static final String PRODUCT_ENRICHED2_TOPIC = "estreams64.ecommerce.products_enriched3";

    public ProductEnrichmentTable2TableService()
    {

    }

    @Override
    public void run() {

        // @TODO Pass in via ENVIRONMENT variables
        final String schemaRegistryUrl = "http://schemaregistry-external.river.svc.cluster.local:8081";
        final String[] boostrapServerList = {
                "broker1-external.river.svc.cluster.local:9093",
                "broker2-external.river.svc.cluster.local:9093",
                "broker3-external.river.svc.cluster.local:9093",
                "broker4-external.river.svc.cluster.local:9093",
                "broker5-external.river.svc.cluster.local:9093",
        };

        final String bootStrapServers = String.join(",", boostrapServerList);

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, CLIENT_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);


        final SpecificAvroSerde<estreams64.ecommerce.products.Value> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Value> productDetailsSerde = new SpecificAvroSerde<>();
        productDetailsSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<estreams64.ecommerce.product_enriched.Value> productEnrichedSerde = new SpecificAvroSerde<>();
        productEnrichedSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KTable<Integer, estreams64.ecommerce.products.Value> productStream = builder.table(PRODUCT_TOPIC, Consumed.with(Serdes.Integer(), productSerde));

        KTable<Integer, Value> productDetailsTable = builder.table(PRODUCT_DETAILS_TOPIC, Consumed.with(Serdes.Integer(), productDetailsSerde));


        KTable<Integer, estreams64.ecommerce.product_enriched.Value> outputTable = productStream.join(productDetailsTable,
          (productValue, productDetailValue) -> ProductEnrichmentStream2StreamService.joiner(productValue, productDetailValue));

        // Filter out some values
        //outputStream.filter((k, v)-> v.getProductId() > 4001);

        KStream<Integer, estreams64.ecommerce.product_enriched.Value> outputStream = outputTable.toStream();

        outputStream.to(PRODUCT_ENRICHED2_TOPIC, Produced.with(Serdes.Integer(), productEnrichedSerde));

        // Sends the contents of the output stream to standard output in realtime
        outputStream.print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
