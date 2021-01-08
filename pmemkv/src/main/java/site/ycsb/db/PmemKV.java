/**
 * Copyright (c) 2013 - 2021 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import io.pmem.pmemkv.*;
import site.ycsb.*;

import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

class ByteArrayConverter implements Converter<byte[]> {
  public ByteArrayConverter() {
  }

  public ByteBuffer toByteBuffer(byte[] entry) {
    return ByteBuffer.wrap(entry);
  }

  public byte[] fromByteBuffer(ByteBuffer entry) {
    byte[] data = new byte[entry.remaining()];
    entry.get(data);
    return data;
  }
}

/**
 * A class that wraps the PmemKV client to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class PmemKV extends DB {
  public static final String ENGINE_PROPERTY = "pmemkv.engine";
  public static final String SIZE_PROPERTY = "pmemkv.dbsize";
  public static final String PATH_PROPERTY = "pmemkv.dbpath";
  private static Database<byte[], byte[]> db = null;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String engineName = props.getProperty(ENGINE_PROPERTY);
    if (engineName == null) {
      engineName = "cmap";
    }
    String path = props.getProperty(PATH_PROPERTY);
    if (path == null) {
      path = "/dev/shm/ycsb";
    }
    String size = props.getProperty(SIZE_PROPERTY);
    if (size == null) {
      size = "1794967296";
    }
    boolean startError = false;
    try {
      db = new Database.Builder<byte[], byte[]>(engineName)
          .setSize(Integer.parseInt(size))
          .setPath(path)
          .setKeyConverter(new ByteArrayConverter())
          .setValueConverter(new ByteArrayConverter())
          .build();
    } catch (DatabaseException e) {
      startError = true;
    }
    if (startError) {
      startError = false;
      try {
        db = new Database.Builder<byte[], byte[]>(engineName)
            .setSize(Integer.parseInt(size))
            .setPath(path)
            .setKeyConverter(new ByteArrayConverter())
            .setValueConverter(new ByteArrayConverter())
            .setForceCreate(true)
            .build();
      } catch (DatabaseException e) {
        startError = true;
      }
      if (startError) {
        throw new DBException("Error while open with " + engineName);
      }
    }
  }

  /**
   * Shutdown the client.
   */
  @Override
  public void cleanup() {
    db.stop();
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      db.get(key.getBytes(), (receivedByteArray) -> {
          deserializeValues(receivedByteArray, fields, result);
        });
    } catch (NotFoundException e) {
      return Status.NOT_FOUND;
    }
    System.out.println("result size: " + result.size());
    return Status.OK;
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    final Map<String, ByteIterator> result = new HashMap<>();
    byte[] currentValues = db.getCopy(key.getBytes(UTF_8));
    if (currentValues == null) {
      return Status.NOT_FOUND;
    }
    deserializeValues(currentValues, null, result);

    //update
    result.putAll(values);

    //store
    try {
      db.put(key.getBytes(UTF_8), serializeValues(result));
      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      db.put(key.getBytes(UTF_8), serializeValues(values));
      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      db.remove(key.getBytes(UTF_8));
    } catch (Exception e) {
      return Status.NOT_FOUND;
    }
    return Status.OK;
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;
      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();
        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }
}
