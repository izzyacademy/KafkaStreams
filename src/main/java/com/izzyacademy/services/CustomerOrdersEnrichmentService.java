package com.izzyacademy.services;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

public class CustomerOrdersEnrichmentService implements StreamMicroService {

    private static final String APP_ID = CustomerOrdersEnrichmentService.class.getName();
    private static final String CLIENT_ID = APP_ID + "." + System.currentTimeMillis();

    private static final String CUSTOMERS = "estreams75.ecommerce.customers";
    private static final String ORDERS = "estreams75.ecommerce.orders";
    private static final String ORDERS_ENRICHED = "estreams75.ecommerce.orders_enriched";

    @Override
    public void run() {

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
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // Serializer/Deserializer for Customers
        final SpecificAvroSerde<estreams75.ecommerce.customers.Value> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);

        // Serializer/Deserializer for Orders
        final SpecificAvroSerde<estreams75.ecommerce.orders.Value> orderSerde = new SpecificAvroSerde<>();
        orderSerde.configure(serdeConfig, false);

        // Serializer/Deserializer for Enriched Order
        final SpecificAvroSerde<estreams75.ecommerce.orders_enriched.Value> orderEnrichedSerde = new SpecificAvroSerde<>();
        orderEnrichedSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        // customer_id, customer details
        GlobalKTable<Integer, estreams75.ecommerce.customers.Value> customersKTable = builder.globalTable(CUSTOMERS, Consumed.with(Serdes.Integer(), customerSerde));

        // customer_id, orders
        KStream<Integer, estreams75.ecommerce.orders.Value> orderStream = builder.stream(ORDERS, Consumed.with(Serdes.Integer(), orderSerde));

        // customer_id, orderEnriched
        KStream<Integer, estreams75.ecommerce.orders_enriched.Value> orderEnrichedStream = orderStream.join(
                customersKTable,
                (orderKey, orderValue) -> keyJoiner(orderKey, orderValue),
                (order, customer) -> joiner(order, customer)
        );

        // order_id, orderEnriched
        KStream<Long, estreams75.ecommerce.orders_enriched.Value> finalStream = orderEnrichedStream
                .map((key, value) -> KeyValue.pair(value.getOrderId(), value));

        finalStream.print(Printed.toSysOut());

        finalStream.to(ORDERS_ENRICHED, Produced.with(Serdes.Long(), orderEnrichedSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Returns a key used to match the key of the Global KTable
     *
     * @param orderKey
     * @param orderValue
     * @return
     */
    private Integer keyJoiner(Integer orderKey, estreams75.ecommerce.orders.Value orderValue) {
        return orderValue.getCustomerId();
    }

    private estreams75.ecommerce.orders_enriched.Value joiner(estreams75.ecommerce.orders.Value order,
                                                              estreams75.ecommerce.customers.Value customer) {
        estreams75.ecommerce.orders_enriched.Value r = new estreams75.ecommerce.orders_enriched.Value();

        r.setOrderId(order.getOrderId());
        r.setCustomerId(order.getCustomerId());
        r.setFirstName(customer.getFirstName());
        r.setLastName(customer.getLastName());
        r.setEmail(customer.getEmail());
        r.setStatus(order.getStatus());
        r.setOrderSource(order.getOrderSource());
        r.setDateCreated(order.getDateCreated());
        r.setDateModified(order.getDateModified());

        return r;
    }
}
