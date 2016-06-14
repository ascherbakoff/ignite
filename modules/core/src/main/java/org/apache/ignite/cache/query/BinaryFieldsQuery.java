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

package org.apache.ignite.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;

/**
 * Scan query over cache entries. Will accept all the entries if no predicate was set.
 *
 * @see IgniteCache#query(Query)
 */
public final class BinaryFieldsQuery extends Query<List<?>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    private final String idxName;

    /** Seek to. */
    private final byte[] seekTo;

    /**
     * @param idxName Index name.
     * @param seekTo Seek to.
     * @param field Field.
     * @param moreFields More fields.
     */
    public BinaryFieldsQuery(@NotNull String idxName, @Nullable byte[] seekTo, String field, String... moreFields) {
        this.idxName = idxName;
        this.seekTo = seekTo;
    }

    /**
     *
     */
    public String idxName() {
        return idxName;
    }

    /**
     *
     */
    public byte[] seekTo() {
        return seekTo;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryFieldsQuery.class, this);
    }
}