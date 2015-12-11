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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// binary object header structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 0)]
    internal struct BinaryObjectHeader : IEquatable<BinaryObjectHeader>
    {
        /** Size, equals to sizeof(BinaryObjectHeader). */
        public const int Size = 24;

        /// <summary>
        /// Flags.
        /// </summary>
        [Flags]
        public enum Flag : short
        {
            /** No flags. */
            None            = 0x00,

            /** Flag: user type. */
            UserType        = 0x01,
            
            /** Flag: schema exists. */
            HasSchema       = 0x02,

            /** Flag indicating that object has raw data. */
            HasRaw          = 0x04,

            /** Flag: offsets take 1 byte. */
            OffsetOneByte   = 0x08,

            /** Flag: offsets take 2 bytes. */
            OffsetTwoBytes  = 0x10,

            /** Flag: compact footer, no field IDs. */
            CompactFooter   = 0x20
        }

        /** Actual header layout */
        public readonly byte Header;        // Header code, always 103 (HdrFull)
        public readonly byte Version;       // Protocol version
        public readonly Flag Flags;         // Flags
        public readonly int TypeId;         // Type ID
        public readonly int HashCode;       // Hash code
        public readonly int Length;         // Length, including header
        public readonly int SchemaId;       // Schema ID (Fnv1 of field type ids)
        public readonly int SchemaOffset;   // Schema offset, or raw offset when RawOnly flag is set.

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryObjectHeader" /> struct.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="hashCode">Hash code.</param>
        /// <param name="length">Length.</param>
        /// <param name="schemaId">Schema ID.</param>
        /// <param name="schemaOffset">Schema offset.</param>
        /// <param name="flags">The flags.</param>
        public BinaryObjectHeader(int typeId, int hashCode, int length, int schemaId, int schemaOffset, Flag flags)
        {
            Header = BinaryUtils.HdrFull;
            Version = BinaryUtils.ProtoVer;

            Debug.Assert(schemaOffset <= length);
            Debug.Assert(schemaOffset >= Size);

            Flags = flags;

            TypeId = typeId;
            HashCode = hashCode;
            Length = length;
            SchemaId = schemaId;
            SchemaOffset = schemaOffset;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryObjectHeader"/> struct from specified stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        private BinaryObjectHeader(IBinaryStream stream)
        {
            Header = stream.ReadByte();
            Version = stream.ReadByte();
            Flags = (Flag) stream.ReadShort();
            Length = stream.ReadInt();
            TypeId = stream.ReadInt();
            HashCode = stream.ReadInt();
            SchemaId = stream.ReadInt();
            SchemaOffset = stream.ReadInt();
        }

        /// <summary>
        /// Writes this instance to the specified stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        private void Write(IBinaryStream stream)
        {
            stream.WriteByte(Header);
            stream.WriteByte(Version);
            stream.WriteShort((short) Flags);
            stream.WriteInt(Length);
            stream.WriteInt(TypeId);
            stream.WriteInt(HashCode);
            stream.WriteInt(SchemaId);
            stream.WriteInt(SchemaOffset);
        }

        /// <summary>
        /// Gets the user type flag.
        /// </summary>
        public bool IsUserType
        {
            get { return (Flags & Flag.UserType) == Flag.UserType; }
        }

        /// <summary>
        /// Gets the schema flag.
        /// </summary>
        public bool HasSchema
        {
            get { return (Flags & Flag.HasSchema) == Flag.HasSchema; }
        }

        /// <summary>
        /// Gets the raw flag.
        /// </summary>
        public bool HasRaw
        {
            get { return (Flags & Flag.HasRaw) == Flag.HasRaw; }
        }

        /// <summary>
        /// Gets the compact footer flag.
        /// </summary>
        public bool IsCompactFooter
        {
            get { return (Flags & Flag.CompactFooter) == Flag.CompactFooter; }
        }

        /// <summary>
        /// Gets the size of the schema field offset (1, 2 or 4 bytes).
        /// </summary>
        public int SchemaFieldOffsetSize
        {
            get
            {
                if ((Flags & Flag.OffsetOneByte) == Flag.OffsetOneByte)
                    return 1;

                if ((Flags & Flag.OffsetTwoBytes) == Flag.OffsetTwoBytes)
                    return 2;

                return 4;
            }
        }

        /// <summary>
        /// Gets the size of the schema field.
        /// </summary>
        public int SchemaFieldSize
        {
            get { return SchemaFieldOffsetSize + 4; }
        }

        /// <summary>
        /// Gets the schema field count.
        /// </summary>
        public int SchemaFieldCount
        {
            get
            {
                if (!HasSchema)
                    return 0;

                var schemaSize = Length - SchemaOffset;

                return schemaSize / SchemaFieldSize;
            }
        }

        /// <summary>
        /// Gets the raw offset of this object in specified stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Raw offset.</returns>
        public int GetRawOffset(IBinaryStream stream, int position)
        {
            Debug.Assert(stream != null);

            if (!HasRaw || !HasSchema)
                return SchemaOffset;

            stream.Seek(position + Length - 4, SeekOrigin.Begin);

            return stream.ReadInt();
        }

        /// <summary>
        /// Reads the schema as dictionary according to this header data.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Schema.</returns>
        public Dictionary<int, int> ReadSchemaAsDictionary(IBinaryStream stream, int position)
        {
            Debug.Assert(stream != null);

            ThrowIfUnsupported();

            var schemaSize = SchemaFieldCount;

            if (schemaSize == 0)
                return null;

            stream.Seek(position + SchemaOffset, SeekOrigin.Begin);

            var schema = new Dictionary<int, int>(schemaSize);

            var offsetSize = SchemaFieldOffsetSize;

            if (offsetSize == 1)
            {
                for (var i = 0; i < schemaSize; i++)
                    schema.Add(stream.ReadInt(), stream.ReadByte());
            }
            else if (offsetSize == 2)
            {
                for (var i = 0; i < schemaSize; i++)
                    schema.Add(stream.ReadInt(), stream.ReadShort());
            }
            else
            {
                for (var i = 0; i < schemaSize; i++)
                    schema.Add(stream.ReadInt(), stream.ReadInt());
            }

            return schema;
        }

        /// <summary>
        /// Reads the schema according to this header data.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Schema.</returns>
        public BinaryObjectSchemaField[] ReadSchema(IBinaryStream stream, int position)
        {
            Debug.Assert(stream != null);

            ThrowIfUnsupported();

            var schemaSize = SchemaFieldCount;

            if (schemaSize == 0)
                return null;

            stream.Seek(position + SchemaOffset, SeekOrigin.Begin);

            var schema = new BinaryObjectSchemaField[schemaSize];

            var offsetSize = SchemaFieldOffsetSize;

            if (offsetSize == 1)
            {
                for (var i = 0; i < schemaSize; i++)
                    schema[i] = new BinaryObjectSchemaField(stream.ReadInt(), stream.ReadByte());
            }
            else if (offsetSize == 2)
            {
                for (var i = 0; i < schemaSize; i++)
                    schema[i] = new BinaryObjectSchemaField(stream.ReadInt(), stream.ReadShort());
            }
            else
            {
                for (var i = 0; i < schemaSize; i++)
                    schema[i] = new BinaryObjectSchemaField(stream.ReadInt(), stream.ReadInt());
            }

            return schema;
        }

        /// <summary>
        /// Writes an array of fields to a stream.
        /// </summary>
        /// <param name="fields">Fields.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="offset">Offset in the array.</param>
        /// <param name="count">Field count to write.</param>
        /// <returns>
        /// Flags according to offset sizes: <see cref="Flag.OffsetOneByte" />,
        /// <see cref="Flag.OffsetTwoBytes" />, or 0.
        /// </returns>
        public static unsafe Flag WriteSchema(BinaryObjectSchemaField[] fields, IBinaryStream stream, int offset,
            int count)
        {
            Debug.Assert(fields != null);
            Debug.Assert(stream != null);
            Debug.Assert(count > 0);
            Debug.Assert(offset >= 0);
            Debug.Assert(offset < fields.Length);

            unchecked
            {
                // Last field is the farthest in the stream
                var maxFieldOffset = fields[offset + count - 1].Offset;

                if (maxFieldOffset <= byte.MaxValue)
                {
                    for (int i = offset; i < count + offset; i++)
                    {
                        var field = fields[i];

                        stream.WriteInt(field.Id);
                        stream.WriteByte((byte)field.Offset);
                    }

                    return Flag.OffsetOneByte;
                }

                if (maxFieldOffset <= ushort.MaxValue)
                {
                    for (int i = offset; i < count + offset; i++)
                    {
                        var field = fields[i];

                        stream.WriteInt(field.Id);

                        stream.WriteShort((short)field.Offset);
                    }

                    return Flag.OffsetTwoBytes;
                }

                if (BitConverter.IsLittleEndian)
                {
                    fixed (BinaryObjectSchemaField* ptr = &fields[offset])
                    {
                        stream.Write((byte*)ptr, count / BinaryObjectSchemaField.Size);
                    }
                }
                else
                {
                    for (int i = offset; i < count + offset; i++)
                    {
                        var field = fields[i];

                        stream.WriteInt(field.Id);
                        stream.WriteInt(field.Offset);
                    }
                }

                return Flag.None;
            }
        }

        /// <summary>
        /// Writes specified header to a stream.
        /// </summary>
        /// <param name="header">The header.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        public static unsafe void Write(BinaryObjectHeader header, IBinaryStream stream, int position)
        {
            Debug.Assert(stream != null);
            Debug.Assert(position >= 0);

            stream.Seek(position, SeekOrigin.Begin);

            if (BitConverter.IsLittleEndian)
                stream.Write((byte*) &header, Size);
            else
                header.Write(stream);
        }

        /// <summary>
        /// Reads an instance from stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Instance of the header.</returns>
        public static unsafe BinaryObjectHeader Read(IBinaryStream stream, int position)
        {
            Debug.Assert(stream != null);
            Debug.Assert(position >= 0);

            stream.Seek(position, SeekOrigin.Begin);

            BinaryObjectHeader hdr;

            if (BitConverter.IsLittleEndian)
            {
                stream.Read((byte*) &hdr, Size);

                Debug.Assert(hdr.Version == BinaryUtils.ProtoVer);
                Debug.Assert(hdr.SchemaOffset <= hdr.Length);
                Debug.Assert(hdr.SchemaOffset >= Size);

            }
            else
                hdr = new BinaryObjectHeader(stream);

            hdr.ThrowIfUnsupported();

            // Only one of the flags can be set
            var f = hdr.Flags;
            Debug.Assert((f & (Flag.OffsetOneByte | Flag.OffsetTwoBytes)) !=
                         (Flag.OffsetOneByte | Flag.OffsetTwoBytes));
            return hdr;
        }

        /** <inheritdoc /> */
        public bool Equals(BinaryObjectHeader other)
        {
            return Header == other.Header &&
                   Version == other.Version &&
                   Flags == other.Flags &&
                   TypeId == other.TypeId &&
                   HashCode == other.HashCode &&
                   Length == other.Length &&
                   SchemaId == other.SchemaId &&
                   SchemaOffset == other.SchemaOffset;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            
            return obj is BinaryObjectHeader && Equals((BinaryObjectHeader) obj);
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Header.GetHashCode();
                hashCode = (hashCode*397) ^ Version.GetHashCode();
                hashCode = (hashCode*397) ^ Flags.GetHashCode();
                hashCode = (hashCode*397) ^ TypeId;
                hashCode = (hashCode*397) ^ HashCode;
                hashCode = (hashCode*397) ^ Length;
                hashCode = (hashCode*397) ^ SchemaId;
                hashCode = (hashCode*397) ^ SchemaOffset;
                return hashCode;
            }
        }

        /** <inheritdoc /> */
        public static bool operator ==(BinaryObjectHeader left, BinaryObjectHeader right)
        {
            return left.Equals(right);
        }

        /** <inheritdoc /> */
        public static bool operator !=(BinaryObjectHeader left, BinaryObjectHeader right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Throws an exception if current header represents unsupported mode.
        /// </summary>
        private void ThrowIfUnsupported()
        {
            // Compact schema is not supported
            if (IsCompactFooter)
                throw new NotSupportedException("Compact binary object footer is not supported in Ignite.NET.");
        }
    }
}
