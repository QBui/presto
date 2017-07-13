/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.parquet.reader;

import com.facebook.presto.hive.parquet.ParquetCorruptionException;
import com.facebook.presto.hive.parquet.ParquetDataSource;
import com.facebook.presto.hive.parquet.RichColumnDescriptor;
import com.facebook.presto.hive.parquet.memory.AggregatedMemoryContext;
import com.facebook.presto.hive.parquet.memory.LocalMemoryContext;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.io.PrimitiveColumnIO;
import parquet.schema.GroupType;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getColumns;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getDescriptor;
import static com.facebook.presto.hive.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static parquet.schema.Type.Repetition.REPEATED;

public class ParquetReader
        implements Closeable
{
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final String MAP_TYPE_NAME = "map";
    private static final String MAP_KEY_NAME = "key";
    private static final String MAP_VALUE_NAME = "value";

    private final MessageType fileSchema;
    private final MessageType requestedSchema;
    private final List<BlockMetaData> blocks;
    private final ParquetDataSource dataSource;
    private final TypeManager typeManager;

    private int currentBlock;
    private BlockMetaData currentBlockMetadata;
    private long currentPosition;
    private long currentGroupRowCount;
    private long nextRowInGroup;
    private int batchSize;
    private final Map<ColumnDescriptor, ParquetColumnReader> columnReadersMap = new HashMap<>();

    private AggregatedMemoryContext currentRowGroupMemoryContext;
    private final AggregatedMemoryContext systemMemoryContext;

    public ParquetReader(MessageType fileSchema,
            MessageType requestedSchema,
            List<BlockMetaData> blocks,
            ParquetDataSource dataSource,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext)
    {
        this.fileSchema = fileSchema;
        this.requestedSchema = requestedSchema;
        this.blocks = blocks;
        this.dataSource = dataSource;
        this.typeManager = typeManager;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();
        initializeColumnReaders();
    }

    @Override
    public void close()
            throws IOException
    {
        currentRowGroupMemoryContext.close();
        dataSource.close();
    }

    public long getPosition()
    {
        return currentPosition;
    }

    public int nextBatch()
    {
        if (nextRowInGroup >= currentGroupRowCount && !advanceToNextRowGroup()) {
            return -1;
        }

        batchSize = toIntExact(min(MAX_VECTOR_LENGTH, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        currentPosition += batchSize;
        for (PrimitiveColumnIO columnIO : getColumns(fileSchema, requestedSchema)) {
            ColumnDescriptor descriptor = columnIO.getColumnDescriptor();
            RichColumnDescriptor column = new RichColumnDescriptor(descriptor.getPath(), columnIO.getType().asPrimitiveType(), descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel());
            ParquetColumnReader columnReader = columnReadersMap.get(column);
            columnReader.prepareNextRead(batchSize);
        }
        return batchSize;
    }

    private boolean advanceToNextRowGroup()
    {
        currentRowGroupMemoryContext.close();
        currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();

        if (currentBlock == blocks.size()) {
            return false;
        }
        currentBlockMetadata = blocks.get(currentBlock);
        currentBlock = currentBlock + 1;

        nextRowInGroup = 0L;
        currentGroupRowCount = currentBlockMetadata.getRowCount();
        columnReadersMap.clear();
        initializeColumnReaders();
        return true;
    }

    public Block readArray(Type type, GroupType field, List<String> path)
            throws IOException
    {
        return readArray(type, field, path, new IntArrayList());
    }

    private Block readArray(Type type, GroupType field, List<String> path, IntList elementOffsets)
            throws IOException
    {
        List<Type> parameters = type.getTypeParameters();
        checkArgument(parameters.size() == 1, "Arrays must have a single type parameter, found %d", parameters.size());
        Type elementType = parameters.get(0);

        path.add(field.getName());
        parquet.schema.Type repeatedType = field.getType(0);
        checkArgument(repeatedType.isRepetition(REPEATED), "Invalid list type %s", field.getName());
        Block block;
        if (isElementType(repeatedType, field.getName())) {
            block = readBlock(elementType, repeatedType, path, elementOffsets);
        }
        else {
            path.add(repeatedType.getName());
            repeatedType = repeatedType.asGroupType().getType(0);
            block = readBlock(elementType, repeatedType, path, elementOffsets);
            path.remove(path.size() - 1);
        }
        path.remove(path.size() - 1);

        if (elementOffsets.isEmpty()) {
            for (int i = 0; i < batchSize; i++) {
                elementOffsets.add(0);
            }
            return RunLengthEncodedBlock.create(elementType, null, batchSize);
        }

        int[] offsets = new int[batchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            offsets[i] = offsets[i - 1] + 1;
        }
        return new ArrayBlock(batchSize, new boolean[batchSize], offsets, block);
    }

    /*
        Here we implement Parquet LIST backwards-compatibility rules.
        See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
     */
    private boolean isElementType(parquet.schema.Type repeatedType, String parentName)
    {
        return (// For legacy 2-level list types with primitive element type, e.g.:
                //
                //    // ARRAY<INT> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated int32 element;
                //    }
                //
                repeatedType.isPrimitive())
                || (
                // For legacy 2-level list types whose element type is a group type with 2 or more fields,
                // e.g.:
                //
                //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated group element {
                //        required binary str (UTF8);
                //        required int32 num;
                //      };
                //    }
                //
                repeatedType.asGroupType().getFieldCount() > 1)
                || (
                // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
                //
                //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated group array {
                //        required binary str (UTF8);
                //      };
                //    }
                //
                repeatedType.getName().equals("array"))
                || (
                // For Parquet data generated by parquet-thrift, e.g.:
                //
                //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
                //    optional group my_list (LIST) {
                //      repeated group my_list_tuple {
                //        required binary str (UTF8);
                //      };
                //    }
                //
                repeatedType.getName().equals(parentName + "_tuple"));
    }

    public Block readMap(Type type, GroupType field, List<String> path)
            throws IOException
    {
        return readMap(type, field, path, new IntArrayList());
    }

    private Block readMap(Type type, GroupType field, List<String> path, IntList elementOffsets)
            throws IOException
    {
        List<Type> parameters = type.getTypeParameters();
        checkArgument(parameters.size() == 2, "Maps must have two type parameters, found %d", parameters.size());
        Block[] blocks = new Block[parameters.size()];

        IntList keyOffsets = new IntArrayList();
        IntList valueOffsets = new IntArrayList();
        parquet.schema.Type repeatedType = field.asGroupType().getType(0);
        path.add(field.getName());
        path.add(repeatedType.getName());
        blocks[0] = readBlock(parameters.get(0), repeatedType.asGroupType().getType(0), path, keyOffsets);
        blocks[1] = readBlock(parameters.get(1), repeatedType.asGroupType().getType(1), path, valueOffsets);
        path.remove(path.size() - 1);
        path.remove(path.size() - 1);

        if (blocks[0].getPositionCount() == 0) {
            for (int i = 0; i < batchSize; i++) {
                elementOffsets.add(0);
            }
            return RunLengthEncodedBlock.create(parameters.get(0), null, batchSize);
        }
        int[] offsets = new int[batchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            int elementPositionCount = keyOffsets.getInt(i - 1);
            elementOffsets.add(elementPositionCount * 2);
            offsets[i] = offsets[i - 1] + elementPositionCount;
        }
        return ((MapType) type).createBlockFromKeyValue(new boolean[batchSize], offsets, blocks[0], blocks[1]);
    }

    public Block readStruct(Type type, GroupType field, List<String> path)
            throws IOException
    {
        return readStruct(type, field, path, new IntArrayList());
    }

    private Block readStruct(Type type, GroupType field, List<String> path, IntList elementOffsets)
            throws IOException
    {
        path.add(field.getName());
        List<TypeSignatureParameter> parameters = type.getTypeSignature().getParameters();
        Block[] blocks = new Block[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            NamedTypeSignature namedTypeSignature = parameters.get(i).getNamedTypeSignature();
            Type fieldType = typeManager.getType(namedTypeSignature.getTypeSignature());
            String name = namedTypeSignature.getName();
            blocks[i] = readBlock(fieldType, field.getType(name), path, new IntArrayList());
        }
        path.remove(path.size() - 1);

        InterleavedBlock interleavedBlock = new InterleavedBlock(blocks);
        int blockSize = blocks[0].getPositionCount();
        int[] offsets = new int[blockSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            elementOffsets.add(parameters.size());
            offsets[i] = i * parameters.size();
        }
        return new ArrayBlock(blockSize, new boolean[blockSize], offsets, interleavedBlock);
    }

    public Block readPrimitive(ColumnDescriptor columnDescriptor, Type type)
            throws IOException
    {
        return readPrimitive(columnDescriptor, type, new IntArrayList());
    }

    private Block readPrimitive(ColumnDescriptor columnDescriptor, Type type, IntList offsets)
            throws IOException
    {
        ParquetColumnReader columnReader = columnReadersMap.get(columnDescriptor);
        if (columnReader.getPageReader() == null) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData metadata = getColumnChunkMetaData(columnDescriptor);
            long startingPosition = metadata.getStartingPos();
            int totalSize = toIntExact(metadata.getTotalSize());
            byte[] buffer = allocateBlock(totalSize);
            dataSource.readFully(startingPosition, buffer);
            ParquetColumnChunkDescriptor descriptor = new ParquetColumnChunkDescriptor(columnDescriptor, metadata, totalSize);
            ParquetColumnChunk columnChunk = new ParquetColumnChunk(descriptor, buffer, 0);
            columnReader.setPageReader(columnChunk.readAllPages());
        }
        return columnReader.readPrimitive(type, offsets);
    }

    private byte[] allocateBlock(int length)
    {
        byte[] buffer = new byte[length];
        LocalMemoryContext blockMemoryContext = currentRowGroupMemoryContext.newLocalMemoryContext();
        blockMemoryContext.setBytes(buffer.length);
        return buffer;
    }

    private ColumnChunkMetaData getColumnChunkMetaData(ColumnDescriptor columnDescriptor)
            throws IOException
    {
        for (ColumnChunkMetaData metadata : currentBlockMetadata.getColumns()) {
            if (metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
                return metadata;
            }
        }
        throw new ParquetCorruptionException("Metadata is missing for column: %s", columnDescriptor);
    }

    private void initializeColumnReaders()
    {
        for (PrimitiveColumnIO columnIO : getColumns(fileSchema, requestedSchema)) {
            ColumnDescriptor descriptor = columnIO.getColumnDescriptor();
            RichColumnDescriptor column = new RichColumnDescriptor(descriptor.getPath(), columnIO.getType().asPrimitiveType(), descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel());
            columnReadersMap.put(column, ParquetColumnReader.createReader(column));
        }
    }

    private Block readBlock(Type type, parquet.schema.Type field, List<String> path, IntList offsets)
            throws IOException
    {
        path.add(field.getName());
        Optional<RichColumnDescriptor> descriptor = getDescriptor(fileSchema, requestedSchema, path);
        if (!descriptor.isPresent()) {
            path.remove(path.size() - 1);
            return RunLengthEncodedBlock.create(type, null, batchSize);
        }
        path.remove(path.size() - 1);

        Block block;
        if (ROW.equals(type.getTypeSignature().getBase())) {
            block = readStruct(type, field.asGroupType(), path, offsets);
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            block = readMap(type, field.asGroupType(), path, offsets);
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            block = readArray(type, field.asGroupType(), path, offsets);
        }
        else {
            block = readPrimitive(descriptor.get(), type, offsets);
        }
        return block;
    }
}
