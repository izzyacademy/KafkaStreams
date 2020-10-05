package com.izzyacademy.core;

import com.izzyacademy.services.KafkaStreamService;
import com.izzyacademy.services.ProductEnrichmentService;

public class Driver {

    public static void main(final String[] args) throws Exception {

        KafkaStreamService service = new ProductEnrichmentService();

        service.run();

    }
}
