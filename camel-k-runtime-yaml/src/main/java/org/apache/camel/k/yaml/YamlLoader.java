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

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.k.RoutesLoader;
import org.apache.camel.k.Runtime;
import org.apache.camel.k.Source;
import org.apache.camel.k.support.URIResolver;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.RoutesDefinition;
import org.apache.camel.model.rest.RestsDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class YamlLoader implements RoutesLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(YamlLoader.class);
    private final Yaml yaml;

    public YamlLoader() {
        this.yaml = new Yaml(new SafeConstructor());
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
                try (InputStream is = URIResolver.resolve(getContext(), source)) {
                    try {
                        RoutesDefinition definition = routesCreator(is);
                        LOGGER.debug("Loaded {} routes from {}", definition.getRoutes().size(), source);
                        setRouteCollection(definition);
                    } catch (Exception e) {
                        LOGGER.debug("Unable to load RoutesDefinition: {}", e.getMessage());
                    }
                }

                try (InputStream is = URIResolver.resolve(getContext(), source)) {
                    try {
                        RestsDefinition definition = restsCreator(is);
                        LOGGER.debug("Loaded {} rests from {}", definition.getRests().size(), source);

                        setRestCollection(definition);
                    } catch (Exception e) {
                        LOGGER.debug("Unable to load RestsDefinition: {}", e.getMessage());
                    }
                }
            }
        };
    }

    private RoutesDefinition routesCreator(InputStream is) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RoutesDefinition routes = new RoutesDefinition();
        Iterable rs = yaml.loadAll(is);
        for (Iterator iterator = rs.iterator(); iterator.hasNext(); ) {
            Object o = iterator.next();
            if (o instanceof Map){
                LOGGER.debug("Creating route ....");
                routes.getRoutes().add(routeCreator((Map) o));
            }
        }
        return routes;
    }

    private RouteDefinition routeCreator(Map<String, Object> map) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        RouteDefinition route = new RouteDefinition();
        Object currentDef = route;
        List routeList = (List) map.get("route");
        for (Object def : routeList){
            Map<String, Object> step = (Map<String, Object>) def;
            String definition = step.keySet().iterator().next();
            Object val = step.get(definition);
            if (val instanceof Map){
                Map<String, Object> args = (Map<String, Object>) step.get(definition);
                Object[] values = args.values().toArray(new Object[0]);
                Class[] classes = args.values().stream().map(o -> o.getClass()).collect(Collectors.toList()).toArray(new Class[0]);
                LOGGER.debug("Multiple arg definition {} : {}", definition, args);
                Method method = currentDef.getClass().getMethod(definition, classes);
                currentDef = method.invoke(currentDef, values);
            } else if (val instanceof String){
                String arg = (String) step.get(definition);
                LOGGER.debug("String arg definition {} : {}", definition, arg);
                Method method = currentDef.getClass().getMethod(definition, String.class);
                currentDef = method.invoke(currentDef, arg);
            } else {
                LOGGER.debug("No arg definition {}", definition);
                Method method = currentDef.getClass().getMethod(definition);
                currentDef = method.invoke(currentDef);
            }
        }
        LOGGER.debug(route.toString());
        return route;
    }

    private RestsDefinition restsCreator(InputStream is) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RestsDefinition rests = new RestsDefinition();
        Iterable rs = yaml.loadAll(is);
        for (Iterator iterator = rs.iterator(); iterator.hasNext(); ) {
            Object o = iterator.next();
            if (o instanceof Map){
                LOGGER.debug("Creating rest ....");
                //TODO: implement rest creator
//                rests.getRests().add(routeCreator((Map) o));
            }
        }
        return rests;
    }
}
