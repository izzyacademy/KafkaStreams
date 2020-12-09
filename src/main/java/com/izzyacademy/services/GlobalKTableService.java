package com.izzyacademy.services;

import estreams75.ecommerce.customers.Value;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

public class GlobalKTableService implements StreamMicroService {

    private static final String CLIENT_ID = "64";
    private static final String APP_ID = GlobalKTableService.class.getName();

    private static final String INPUT_TOPIC = "estreams75.ecommerce.customers";

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
        //streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final SpecificAvroSerde<Value> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<Integer, Value> customersKTable = builder.globalTable(INPUT_TOPIC, Consumed.with(Serdes.Integer(), customerSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        String stateStoreName = customersKTable.queryableStoreName();

        ReadOnlyKeyValueStore view = streams.store(stateStoreName, QueryableStoreTypes.keyValueStore());

        KeyValueIterator a = view.all();

        while (a.hasNext()) {
            System.out.println(a.next());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
