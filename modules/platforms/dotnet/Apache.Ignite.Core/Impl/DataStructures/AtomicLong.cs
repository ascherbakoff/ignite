﻿/*
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

namespace Apache.Ignite.Core.Impl.DataStructures
{
    using System.Diagnostics;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;

    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Atomic long wrapper.
    /// </summary>
    internal sealed class AtomicLong : PlatformTarget, IAtomicLong
    {
        /** */
        private readonly string _name;

        /** Operation codes. */
        private enum Op
        {
            AddAndGet = 1,
            Close = 2,
            CompareAndSetAndGet = 4,
            DecrementAndGet = 5,
            Get = 6,
            GetAndSet = 10,
            IncrementAndGet = 11,
            IsClosed = 12
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicLong"/> class.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="marsh">The marshaller.</param>
        /// <param name="name">The name.</param>
        public AtomicLong(IUnmanagedTarget target, Marshaller marsh, string name) : base(target, marsh)
        {
            Debug.Assert(!string.IsNullOrEmpty(name));

            _name = name;
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public long Read()
        {
            return DoOutOp((int) Op.Get);
        }

        /** <inheritDoc /> */
        public long Increment()
        {
            return DoOutOp((int) Op.IncrementAndGet);
        }

        /** <inheritDoc /> */
        public long Add(long value)
        {
            return DoOutInOpLong((int) Op.AddAndGet, value);
        }

        /** <inheritDoc /> */
        public long Decrement()
        {
            return DoOutOp((int) Op.DecrementAndGet);
        }

        /** <inheritDoc /> */
        public long Exchange(long value)
        {
            return DoOutInOpLong((int) Op.GetAndSet, value);
        }

        /** <inheritDoc /> */
        public long CompareExchange(long value, long comparand)
        {
            return DoOutOp((int) Op.CompareAndSetAndGet, (IBinaryStream s) =>
            {
                s.WriteLong(comparand);
                s.WriteLong(value);
            });
        }

        /** <inheritDoc /> */
        public void Close()
        {
            DoOutOp((int) Op.Close);
        }

        /** <inheritDoc /> */
        public bool IsClosed()
        {
            return DoOutOp((int) Op.IsClosed) == True;
        }
    }
}