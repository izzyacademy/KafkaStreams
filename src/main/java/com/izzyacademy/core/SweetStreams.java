package com.izzyacademy.core;

import com.izzyacademy.services.KafkaStreamService;

// Run compiled application as
// java -jar .\target\streams-app-1.0.0-uber.jar
public class SweetStreams {

    public static void main(final String[] args) throws Exception {

        int selectionIndex = 0; // which service are we going to run?

        if (args.length == 1) {
            selectionIndex = Integer.parseInt(args[0]);
        }

        // These are the names of Micro Services that process Kafka Streams from Topics
        final String[] serviceNames = {
                "com.izzyacademy.services.ProductEnrichmentStream2StreamService",
                "com.izzyacademy.services.ProductEnrichmentTable2StreamService",
                "com.izzyacademy.services.ProductEnrichmentTable2TableService",
        };

        // Select one of the micro services listed above
        final String serviceName = serviceNames[selectionIndex];

        System.out.println();
        System.out.println("selectionIndex=" + selectionIndex + ", serviceName=" + serviceName);
        System.out.println();

        // All classes must implement the KafkaStreamService interface
        KafkaStreamService service = (KafkaStreamService) Class.forName(serviceName).newInstance();

        // Run the service
        service.run();
    }
}
