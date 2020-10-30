package com.izzyacademy.core;

import com.izzyacademy.services.KafkaStreamService;
import com.izzyacademy.services.ProductTransformService;

public class Driver {

    public static void main(final String[] args) throws Exception {

        final String[] serviceNames = {
                "com.izzyacademy.services.ProductEnrichmentService",
                "com.izzyacademy.services.ProductTransformService"
        };

        final String serviceName = serviceNames[0];

        // All classes must implement the KafkaStreamService interface
        KafkaStreamService service = (KafkaStreamService) Class.forName(serviceName).newInstance();

        service.run();
    }
}
