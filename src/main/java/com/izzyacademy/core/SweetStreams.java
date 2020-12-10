package com.izzyacademy.core;

import com.izzyacademy.services.*;

import java.util.HashMap;
import java.util.Map;

// Run compiled application as
// java -jar .\target\streams-app-1.0.0-uber.jar
public class SweetStreams {

    private static final String DEFAULT_SERVICE = DefaultService.class.getSimpleName();

    private static final Map<String, String> serviceNames = new HashMap<>(8);

    static {

        // Register the Micro Services
        registerMicroService(DefaultService.class);

        registerMicroService(CustomerOrdersEnrichmentService.class);
        registerMicroService(OrderLineItemEnrichmentService.class);

        registerMicroService(ProductEnrichmentTable2TableService.class);
        registerMicroService(ProductEnrichmentStream2StreamService.class);
        registerMicroService(ProductEnrichmentTable2StreamService.class);
    }

    /**
     * Registers the MicroService in the registry of service names
     *
     * @param microServiceClass Micro Service Class
     */
    private static void registerMicroService(Class<? extends StreamMicroService> microServiceClass) {
        serviceNames.put(microServiceClass.getSimpleName(), microServiceClass.getName());
    }

    public static void main(final String[] args) throws Exception {

        // Retrieving the environment variables
        final Map<String, String> env = System.getenv();

        // Picking the service name from the environment variable
        final String serviceName = env.getOrDefault("SERVICE_NAME", DEFAULT_SERVICE);

        // Select one of the registered micro services
        final String serviceClass = serviceNames.get(serviceName);

        System.out.println("Registered Micro Services");
        System.out.println(serviceNames.entrySet());
        System.out.println();
        System.out.println("selectedServiceName=" + serviceName + ", serviceClass=" + serviceClass);
        System.out.println();

        // All classes must implement the KafkaStreamService interface
        StreamMicroService service = (StreamMicroService) Class.forName(serviceClass).newInstance();

        // Run the service
        service.run();
    }
}
