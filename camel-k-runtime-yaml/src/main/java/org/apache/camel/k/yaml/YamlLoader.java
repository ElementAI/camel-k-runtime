/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.k.yaml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.k.RoutesLoader;
import org.apache.camel.k.Runtime;
import org.apache.camel.k.Source;
import org.apache.camel.k.support.URIResolver;
import org.apache.camel.model.RoutesDefinition;
import org.apache.camel.model.rest.RestsDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.UnmarshalException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class YamlLoader implements RoutesLoader {
    private final ObjectMapper mapper;
    private static final Logger LOGGER = LoggerFactory.getLogger(YamlLoader.class);

    private static final String ROOT_ELEMENT = "routes";
    private static final String NAMESPACE_LABEL = "xmlns";
    private static final String NAMESPACE_VALUE = "http://camel.apache.org/schema/spring";
    private static final String ATTR_KEY = "attr";

    public YamlLoader() {
        YAMLFactory yamlFactory = new YAMLFactory()
                .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
                .configure(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS, true)
                .configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false);

        this.mapper = new ObjectMapper(yamlFactory)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public List<String> getSupportedLanguages() {
        return Collections.singletonList("yaml");
    }

    @Override
    public RouteBuilder load(Runtime.Registry registry, Source source) throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                try (InputStream is = URIResolver.resolve(getContext(), source); InputStream xis = yamlToXml(is)) {
                    try {
                        RoutesDefinition definition = getContext().loadRoutesDefinition(xis);
                        LOGGER.debug("Loaded {} routes from {}", definition.getRoutes().size(), source);
                        setRouteCollection(definition);
                    } catch (IllegalArgumentException e) {
                        // ignore
                    } catch (UnmarshalException e) {
                        LOGGER.debug("Unable to load RoutesDefinition: {}", e.getMessage());
                    }
                }

                try (InputStream is = URIResolver.resolve(getContext(), source); InputStream xis = yamlToXml(is)) {
                    try {
                        RestsDefinition definition = getContext().loadRestsDefinition(xis);
                        LOGGER.debug("Loaded {} rests from {}", definition.getRests().size(), source);

                        setRestCollection(definition);
                    } catch (IllegalArgumentException e) {
                        // ignore
                    } catch (UnmarshalException e) {
                        LOGGER.debug("Unable to load RestsDefinition: {}", e.getMessage());
                    }
                }
            }
        };
    }

    private InputStream yamlToXml(InputStream is) throws Exception {
        String yaml = new String(toByteArray(is));
        LinkedHashMap routeMap = mapper.readValue(yaml, LinkedHashMap.class);

        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
        StringWriter out = new StringWriter();
        XMLStreamWriter sw = xmlOutputFactory.createXMLStreamWriter(out);

        sw.writeStartDocument();
        sw.writeStartElement(ROOT_ELEMENT);
        sw.writeAttribute(NAMESPACE_LABEL, NAMESPACE_VALUE);

        writeElementMap(sw, routeMap);
        sw.writeEndElement();
        sw.writeEndDocument();

        return new ByteArrayInputStream(out.toString().getBytes());
    }

    private void writeElementMap(XMLStreamWriter sw, LinkedHashMap<String, Object> map) throws XMLStreamException {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().equals(ATTR_KEY) && entry.getValue() instanceof LinkedHashMap) {
                writeAttributesMap(sw, (LinkedHashMap<String, Object>) entry.getValue());
            } else if (entry.getValue() instanceof LinkedHashMap) {
                sw.writeStartElement(entry.getKey());
                writeElementMap(sw, (LinkedHashMap<String, Object>) entry.getValue());
                sw.writeEndElement();
            } else {
                sw.writeStartElement(entry.getKey());
                sw.writeCharacters(entry.getValue().toString());
                sw.writeEndElement();
            }
        }
    }

    private void writeAttributesMap(XMLStreamWriter sw, LinkedHashMap<String, Object> map) throws XMLStreamException {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            sw.writeAttribute(entry.getKey(), entry.getValue().toString());
        }
    }

    private byte[] toByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) != -1) {
            os.write(buffer, 0, len);
        }
        return os.toByteArray();
    }
}
