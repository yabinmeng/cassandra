/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.LongConsumer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public abstract class MemoryIndex
{
    protected final IndexContext indexContext;

    protected MemoryIndex(IndexContext indexContext)
    {
        this.indexContext = indexContext;
    }

    public abstract void add(DecoratedKey key,
                             Clustering clustering,
                             ByteBuffer value,
                             LongConsumer onHeapAllocationsTracker,
                             LongConsumer offHeapAllocationsTracker);

    public abstract RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange);

    public abstract ByteBuffer getMinTerm();

    public abstract ByteBuffer getMaxTerm();

    /**
     * Iterate all Term->PrimaryKeys mappings in sorted order
     */
    public abstract Iterator<Pair<ByteComparable, PrimaryKeys>> iterator();
}
