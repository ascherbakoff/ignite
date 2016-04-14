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

package org.apache.ignite.internal;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.*;

/**
 * The GridGetOrStartSelfTest tests get or start semantics.
 */

@GridCommonTest(group = "Kernal Self")
public class GridGetOrStartSelfTest extends GridCommonAbstractTest {
    /** Concurrency. */
    public static final int CONCURRENCY = 10;

    /**
     * Default constructor.
     */
    public GridGetOrStartSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests default grid
     */
    public void testDefaultGridGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration(null);

        try (Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);

                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
            }

            Ignite ignite2 = Ignition.getOrStart(cfg);

            assertEquals("Must return same instance", ignite, ignite2);
        }

        assertTrue(G.allGrids().isEmpty());
    }

    /**
     * Tests named grid
     */
    public void testNamedGridGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration("test");
        try (Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);

                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
                // No-op.
            }

            Ignite ignite2 = Ignition.getOrStart(cfg);

            assertEquals("Must return same instance", ignite, ignite2);
        }

        assertTrue(G.allGrids().isEmpty());
    }

    /**
     * Tests concurrent grid initialization
     */
    public void testConcurrentGridGetOrStartCon() throws Exception {
        final IgniteConfiguration cfg = getConfiguration(null);

        AtomicReference<Ignite> ref = new AtomicReference<>();

        try {
            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    // must return same instance in each thread

                    try {
                        Ignite ignite = Ignition.getOrStart(cfg);

                        boolean set = ref.compareAndSet(null, ignite);

                        if (!set)
                            assertEquals(ref.get(), ignite);
                    }
                    catch (IgniteCheckedException e) {
                        throw new RuntimeException("Ignite error", e);
                    }
                }
            }, CONCURRENCY, "GridCreatorThread");
        }
        catch (Exception e) {
            fail("Exception is not expected");
        }

        G.stopAll(true);

        assertTrue(G.allGrids().isEmpty());
    }
}
