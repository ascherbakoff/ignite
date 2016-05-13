/*
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

package org.apache.ignite.examples.events;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * Note that ignite events are disabled by default and must be specifically enabled,
 * just like in {@code examples/config/example-ignite.xml} file.
 * <p>
 * Remote nodes should always be started with configuration: {@code 'ignite.sh examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start
 * node with {@code examples/config/example-ignite.xml} configuration.
 */
public class EvictEventExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();
            cfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
            cfg.setEvictionPolicy(new FifoEvictionPolicy(1));


            System.out.println();
            System.out.println(">>> Events API example started.");

            // Listen to events happening on local node.
            IgnitePredicate<CacheEvent> lsnr = new IgnitePredicate<CacheEvent>() {
                @Override public boolean apply(CacheEvent evt) {
                    System.out.println("Received task event [evt=" + evt.name() + ", taskName=" + evt.taskName() + ']');

                    return true; // Return true to continue listening.
                }
            };

            // Register event listener for all local task execution events.
            ignite.events().localListen(lsnr, EVT_CACHE_ENTRY_EVICTED);


            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cfg);

            cache.put("1", null);
            cache.put("2", "2");

            // Unsubscribe local task event listener.
            ignite.events().stopLocalListen(lsnr);
        }
    }
}