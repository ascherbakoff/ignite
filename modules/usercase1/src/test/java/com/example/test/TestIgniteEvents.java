package com.example.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class TestIgniteEvents {


    AtomicInteger count = new AtomicInteger();

    Ignite server, client;


    @Before
    public void setup() throws IgniteCheckedException {
        IgnitePredicate<CacheEvent> locLsnr = (CacheEvent evt) -> {
//            System.out.println("got event "+evt);
            if (evt.oldValue() != null) count.incrementAndGet();
            return true; // Continue listening.
        };


        Ignition.setClientMode(false);
        server = IgnitionEx.start("ignite-config.xml", "server");
        server.events().enableLocal(EventType.EVT_CACHE_ENTRY_EVICTED);
        server.events().localListen(locLsnr, EventType.EVT_CACHE_ENTRY_EVICTED);

        Ignition.setClientMode(true);
        client = IgnitionEx.start("ignite-config.xml", "client");
    }

    @After
    public void teardown(){
        System.out.println("shutting down...");
        if (client != null){
            client.close();
        }

        if (server != null){
            server.close();
        }
    }

    @Test
    public void testEvictionEventsWithValues() throws InterruptedException {
        CacheConfiguration<String, String> cfg = new CacheConfiguration<>();
        cfg.setName("USERS");
        final IgniteCache<String, String> cache = client.getOrCreateCache(cfg);

        //put 100 users into the cache
        IntStream.range(0,100).forEach(value -> cache.put("key - "+value, "value - "+value));

        //make sure they are in the cache
        IntStream.range(0,100).forEach(value -> Assert.assertEquals("value - "+value, cache.get("key - "+value)));

        Assert.assertEquals(0, count.get());

        //put 100 more and we would expect 100 eviction events with values.
        IntStream.range(100,200).forEach(value -> cache.put("key - "+value, "value - "+value));

        Assert.assertEquals(100, count.get());
    }
}
