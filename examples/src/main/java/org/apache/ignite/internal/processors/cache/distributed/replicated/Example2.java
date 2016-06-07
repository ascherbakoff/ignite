package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class Example2 {
    public static void main(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        cfg.setDiscoverySpi(spi);

        CacheConfiguration<Integer, Person> ccfg1 = new CacheConfiguration<>();
        ccfg1.setName("P");
        ccfg1.setIndexedTypes(Integer.class, Person.class);

        CacheConfiguration<Integer, Department> ccfg2 = new CacheConfiguration<>();
        ccfg2.setName("D");
        ccfg2.setIndexedTypes(Integer.class, Department.class);

        CacheConfiguration<Integer, Org> ccfg3 = new CacheConfiguration<>();
        ccfg3.setName("O");
        ccfg3.setIndexedTypes(Integer.class, Org.class);

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);
        try(Ignite ignite = Ignition.getOrStart(cfg)) {
            IgniteCache<Integer, Person> cache = ignite.cache("P");
            QueryCursor<List<?>> query = cache.query(new SqlFieldsQuery(
                "select P.Person.*,dep.*,org.* from P.Person inner join D.Department dep ON dep.id=P.Person.depId left join O.Org org ON org.id=dep.orgId"
            ));
            System.out.println(query.getAll());
        }
    }

    public static class Person {
        private int id;
        private int depId;
        private String name;

        @QuerySqlField
        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @QuerySqlField
        public int getDepId() {
            return depId;
        }

        public void setDepId(int depId) {
            this.depId = depId;
        }

        @QuerySqlField
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class Department {
        private int id;
        private int orgId;
        private String name;

        @QuerySqlField(index = true)
        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @QuerySqlField(index = true)
        public int getOrgId() {
            return orgId;
        }

        public void setOrgId(int orgId) {
            this.orgId = orgId;
        }

        @QuerySqlField
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class Org {
        private int id;
        private String name;

        @QuerySqlField(index = true)
        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @QuerySqlField
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
