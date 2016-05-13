/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package db;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 *
 */
public class Organization implements Serializable {
    /** */
    @QuerySqlField(index = true)
    private int id;

    /** */
    @QuerySqlField(index = true)
    private String name;

    public Organization() {
        // No-op.
    }

    public Organization(int id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * @return ID.
     */
    public int getId() {
        return id;
    }

    /**
     * @param id ID.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return Name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Organization [id=" + id + ", name=" + name + ']';
    }
}
