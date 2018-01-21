/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streams;

import ml.ModelBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class AirlinePredictorProcessor extends AbstractProcessor<String, String> {

    private MeteredKeyValueStore<String, List<String>> flights;
    private static final Logger LOG = LoggerFactory.getLogger(AirlinePredictorProcessor.class);

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        flights = (MeteredKeyValueStore) context().getStateStore("flights");
        context().schedule(10000L);
    }

    @Override
    public void process(String airportId, String flightData) {
        List<String> flightList = this.flights.get(airportId);
        if (flightList == null) {
            flightList = new ArrayList<>();
        }
        LOG.debug("Adding key {}", airportId);
        flightList.add(flightData);
        this.flights.put(airportId, flightList);
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, List<String>> allFlights = flights.all();
        while (allFlights.hasNext()) {
            KeyValue<String, List<String>> kv = allFlights.next();

            List<String> flightList = kv.value;
            String airportCode = kv.key;
            LOG.debug("Found Key {}", airportCode);
            if(flightList.size() >= 100){
               try {
                   LOG.debug("sending flight list {}", flightList);
                   byte[] serializedRegression = ModelBuilder.train(flightList);
                   context().forward(airportCode, serializedRegression);
                   LOG.info("updating model for {}", airportCode);
                   flightList.clear();
                   flights.put(airportCode, flightList);
               }catch (Exception e) {
                   LOG.error("couldn't update online regression for {}",airportCode, e);
               }
            }
        }
      allFlights.close();
    }

}
