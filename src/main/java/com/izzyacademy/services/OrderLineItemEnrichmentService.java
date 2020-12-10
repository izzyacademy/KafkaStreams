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

public class OrderLineItemEnrichmentService implements StreamMicroService {

    private static final String APP_ID = OrderLineItemEnrichmentService.class.getName();
    private static final String CLIENT_ID = APP_ID + "." + System.currentTimeMillis();

    private static final String PRODUCT_SKUS = "estreams85.ecommerce.product_skus";
    private static final String ORDER_ITEMS = "estreams75.ecommerce.order_items";
    private static final String ORDER_ITEMS_ENRICHED = "estreams75.ecommerce.order_items_enriched";

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
        final SpecificAvroSerde<estreams85.ecommerce.product_skus.Value> productSkuSerde = new SpecificAvroSerde<>();
        productSkuSerde.configure(serdeConfig, false);

        // Serializer/Deserializer for Orders
        final SpecificAvroSerde<estreams75.ecommerce.order_items.Value> orderItemSerde = new SpecificAvroSerde<>();
        orderItemSerde.configure(serdeConfig, false);

        // Serializer/Deserializer for Enriched Order
        final SpecificAvroSerde<estreams75.ecommerce.order_items_enriched.Value> orderItemEnrichedSerde = new SpecificAvroSerde<>();
        orderItemEnrichedSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        // sku_id, sku details
        GlobalKTable<String, estreams85.ecommerce.product_skus.Value> productSkuTable = builder.globalTable(PRODUCT_SKUS, Consumed.with(Serdes.String(), productSkuSerde));

        // customer_id, orders
        KStream<Integer, estreams75.ecommerce.order_items.Value> orderLineItemStream = builder.stream(ORDER_ITEMS, Consumed.with(Serdes.Integer(), orderItemSerde));

        // customer_id, orderItemEnriched
        KStream<Integer, estreams75.ecommerce.order_items_enriched.Value> orderItemEnrichedStream = orderLineItemStream.join(
                productSkuTable,
                (orderItemKey, orderItemValue) -> keyJoiner(orderItemKey, orderItemValue),
                (orderItem, productSku) -> joiner(orderItem, productSku)
        );

        // order_id, orderEnriched
        KStream<Long, estreams75.ecommerce.order_items_enriched.Value> finalStream = orderItemEnrichedStream
                .map((key, orderItem) -> KeyValue.pair(orderItem.getOrderLineItemId(), orderItem));

        finalStream.print(Printed.toSysOut());

        finalStream.to(ORDER_ITEMS_ENRICHED, Produced.with(Serdes.Long(), orderItemEnrichedSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     *
     * @param orderItemKey
     * @param orderItemValue
     * @return
     */
    private String keyJoiner(Integer orderItemKey, estreams75.ecommerce.order_items.Value orderItemValue) {
        return orderItemValue.getSkuId();
    }

    /**
     *
     * @param orderItemValue
     * @param productSkuValue
     * @return
     */
    private estreams75.ecommerce.order_items_enriched.Value joiner( estreams75.ecommerce.order_items.Value orderItemValue,
                                                               estreams85.ecommerce.product_skus.Value productSkuValue) {
        estreams75.ecommerce.order_items_enriched.Value r = new estreams75.ecommerce.order_items_enriched.Value();

        int itemCount = orderItemValue.getItemCount();
        double skuPrice = productSkuValue.getSkuPrice();
        double subTotal = itemCount * skuPrice;

        r.setOrderLineItemId(orderItemValue.getOrderLineItemId());
        r.setItemCount(orderItemValue.getItemCount());
        r.setSkuId(productSkuValue.getSkuId());
        r.setSkuPrice(productSkuValue.getSkuPrice());
        r.setSkuDescription(productSkuValue.getSkuDescription());
        r.setSubTotal(subTotal);
        r.setProductId(productSkuValue.getProductId());
        r.setOrderId(orderItemValue.getOrderId());
        r.setCustomerId(orderItemValue.getCustomerId());
        r.setStatus(orderItemValue.getStatus());
        r.setDateCreated(orderItemValue.getDateCreated());
        r.setDateModified(orderItemValue.getDateModified());

        return r;
    }
}
