/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package db;

import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 * This example demonstrates use of GridGain database persistence. <p> To execute this example you should start an
 * instance of {@code DbExampleNodeStartup} class which will start up a GridGain remote server node with a proper
 * configuration. <p> When {@code UPDATE} parameter of this example is set to {@code true}, the example will populate
 * the cache with some data and will then run an example SQL query to fetch some results. <p> When {@code UPDATE}
 * parameter of this example is set to {@code false}, the example will run the SQL query against the cache without the
 * initial data population. You can populate the cache first, then restart the nodes and run the example with {@code
 * UPDATE} set to {@code false} to verify the persistence. <p> Note that for now the amount of nodes in the cluster
 * between restarts must remain the same in order to get correct results. To clean previously created persistence
 * folders, you should delete the {@code IGNITE_HOME/work/db} folder. Note also that in order for the results to be
 * persisted correctly, GridGain server nodes should be shut down gracefully.
 */
public class DbExample {
    /** */
    private static final boolean UPDATE = true;

    public static void main(String[] args) throws Exception {
        Ignition.setClientMode(true);

        try (Ignite ig = Ignition.start("examples/config/db-server.xml")) {

            IgniteCache<String, BinaryObject> cache = ig.cache("deposit").withKeepBinary();

            if (UPDATE) {
                System.out.println("Populating the cache...");

                long start = System.nanoTime(), t1 = start;
                try (IgniteDataStreamer<String, BinaryObject> streamer = ig.dataStreamer("deposit")) {
                    streamer.allowOverwrite(true);

                    for (int i = 1; i <= 100_000; i++) {
                        BinaryObjectBuilder builder = Ignition.ignite().binary().builder("Deposit");
                        String sid = i + "";
                        builder.setField("ID", sid);
                        builder.setField("PERSON_ID", sid);
                        builder.setField("BALANCE", i * 1_000.);
                        builder.setField("DESCRIPTION", RandomStringUtils.randomAlphabetic(1024));
                        builder.setField("OPENDAY", new java.sql.Date(2010 - 1900, 0, 1));

                        streamer.addData(String.valueOf(i), builder.build());

                        if (i > 0 && i % 10_000 == 0) {
                            double secs = (System.nanoTime() - t1) / 1000 / 1000 / 1000.;
                            System.out.println("Done: " + i + " time(sec):" + secs + " op/sec:" + Math.round(10_000 / secs * 1000 ) / 1000.);
                            t1 = System.nanoTime();
                        }
                    }
                }

                System.out.println("total(sec): " + (System.nanoTime() - start) / 1000 / 1000 / 1000.);

                System.out.println("cache size = " + cache.size(CachePeekMode.ALL));

                String testKey = "54321";
                BinaryObject orgBin = cache.get(testKey);
                System.out.println("GET result: " + orgBin);

                QueryCursor<List<?>> cur = cache.query(
                    new SqlFieldsQuery("select id, balance, description from Deposit where id = ?").setArgs(testKey));

                System.out.println("SQL Result: " + cur.getAll());
            }
        }
    }
}
