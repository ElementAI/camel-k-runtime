/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.knative;

import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.DelegateEndpoint;
import org.apache.camel.Endpoint;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.cloud.ServiceDefinition;
import org.apache.camel.component.knative.ce.CloudEventsProcessors;
import org.apache.camel.k.adapter.DefaultEndpoint;
import org.apache.camel.k.adapter.Exceptions;
import org.apache.camel.k.adapter.Services;
import org.apache.camel.processor.Pipeline;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.util.StringHelper;
import org.apache.camel.util.URISupport;

import java.util.HashMap;
import java.util.Map;


@UriEndpoint(
    firstVersion = "3.0.0",
    scheme = "knative",
    syntax = "knative:type/target",
    title = "Knative",
    label = "cloud,eventing")
public class KnativeEndpoint extends DefaultEndpoint implements DelegateEndpoint {
    @UriPath(description = "The Knative type")
    private final Knative.Type type;

    @UriPath(description = "The Knative name")
    private final String name;

    @UriParam
    private final KnativeConfiguration configuration;

    private final KnativeEnvironment environment;
    private final KnativeEnvironment.KnativeServiceDefinition service;
    private final Endpoint endpoint;

    public KnativeEndpoint(String uri, KnativeComponent component, Knative.Type targetType, String remaining, KnativeConfiguration configuration) {
        super(uri, component);

        this.type = targetType;
        this.name = remaining.indexOf('/') != -1 ? StringHelper.before(remaining, "/") : remaining;
        this.configuration = configuration;
        this.environment =  this.configuration.getEnvironment();
        this.service = this.environment.lookupServiceOrDefault(targetType, remaining);

        switch (service.getProtocol()) {
        case http:
        case https:
            this.endpoint = http(component.getCamelContext(), service);
            break;
        default:
            throw new IllegalArgumentException("unsupported protocol: " + this.service.getProtocol());
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        Services.start(endpoint);
    }

    @Override
    protected void doStop() throws Exception {
        Services.stop(endpoint);
        super.doStop();
    }

    @Override
    public KnativeComponent getComponent() {
        return (KnativeComponent) super.getComponent();
    }

    @Override
    public Producer createProducer() throws Exception {
        final String version = configuration.getCloudEventsSpecVersion();
        final Processor ceProcessor = CloudEventsProcessors.forSpecversion(version).producerProcessor(this);
        final Processor ceConverter = new KnativeConversionProcessor(configuration.isJsonSerializationEnabled());

        return new KnativeProducer(this, ceProcessor, ceConverter, endpoint.createProducer());
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        final String version = configuration.getCloudEventsSpecVersion();
        final Processor ceProcessor = CloudEventsProcessors.forSpecversion(version).consumerProcessor(this);
        final Processor pipeline = Pipeline.newInstance(getCamelContext(), ceProcessor, processor);
        final Consumer consumer = endpoint.createConsumer(pipeline);

        configureConsumer(consumer);

        return consumer;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public Endpoint getEndpoint() {
        return this.endpoint;
    }

    public Knative.Type getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public KnativeEnvironment.KnativeServiceDefinition getService() {
        return service;
    }

    // *****************************
    //
    // Helpers
    //
    // *****************************

    private static Endpoint http(CamelContext context, ServiceDefinition definition) {
        try {
            final String scheme = Knative.HTTP_COMPONENT;
            final String protocol = definition.getMetadata().getOrDefault(Knative.KNATIVE_PROTOCOL, "http");

            String host = definition.getHost();
            int port = definition.getPort();

            if (ObjectHelper.isEmpty(host)) {
                String name = definition.getName();
                String zone = definition.getMetadata().get(Knative.SERVICE_META_ZONE);

                if (ObjectHelper.isNotEmpty(zone)) {
                    try {
                        zone = context.resolvePropertyPlaceholders(zone);
                    } catch (IllegalArgumentException e) {
                        zone = null;
                    }
                }
                if (ObjectHelper.isNotEmpty(zone)) {
                    name = name + "." + zone;
                }

                host = name;
            }

            ObjectHelper.notNull(host, Knative.SERVICE_META_HOST);
            ObjectHelper.notNull(protocol, Knative.KNATIVE_PROTOCOL);

            String uri = String.format("%s:%s://%s", scheme, protocol, host);
            if (port != -1) {
                uri = uri + ":" + port;
            }

            String path = definition.getMetadata().get(Knative.SERVICE_META_PATH);
            if (path != null) {
                if (!path.startsWith("/")) {
                    uri += "/";
                }

                uri += path;
            }

            final String filterKey = definition.getMetadata().get(Knative.FILTER_HEADER_NAME);
            final String filterVal = definition.getMetadata().get(Knative.FILTER_HEADER_VALUE);
            final Map<String, Object> parameters = new HashMap<>();

            if (ObjectHelper.isNotEmpty(filterKey) && ObjectHelper.isNotEmpty(filterVal)) {
                parameters.put("filter.headerName", filterKey);
                parameters.put("filter.headerValue", filterVal);
            }

            // configure netty to use relative path instead of full
            // path that is the default to make istio working
            parameters.put("useRelativePath", "true");

            uri = URISupport.appendParametersToURI(uri, parameters);

            return context.getEndpoint(uri);
        } catch (Exception e) {
            throw Exceptions.wrapRuntimeCamelException(e);
        }
    }
}
