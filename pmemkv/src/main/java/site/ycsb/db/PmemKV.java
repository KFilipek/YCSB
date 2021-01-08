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

class ByteConverter implements Converter<byte[]> {

  @Override
  public ByteBuffer toByteBuffer(byte[] bytes) {
    return ByteBuffer.wrap(bytes);
  }

  @Override
  public byte[] fromByteBuffer(ByteBuffer byteBuffer) {
    byte[] data = new byte[byteBuffer.remaining()];
    byteBuffer.get(data);
    return data;

  }
}

class MapToByteBufferConverter implements Converter<Map<String, ByteIterator>> {
  public MapToByteBufferConverter() {
  }

  @Override
  public ByteBuffer toByteBuffer(Map<String, ByteIterator> entries) {
    System.out.println("Serialization step...");
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> entry : entries.entrySet()) {
        final byte[] keyBytes = entry.getKey().getBytes(UTF_8);
        final byte[] valueBytes = entry.getValue().toArray();
        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);
        System.out.println("Key: " + entry.getKey());
        System.out.println("Lengths: " + keyBytes.length + " " + valueBytes.length);
        buf.clear();
      }
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Map<String, ByteIterator> fromByteBuffer(ByteBuffer input) {
    System.out.println("Deserialization step...");
    Map<String, ByteIterator> result = new HashMap<>();

    int offset = 0;
    while (input.remaining() != 0) {
      final int keyLen = input.getInt(); // increments position by 4
      offset += 4;
      byte[] values = new byte[keyLen];
      System.out.println("KeyLen: " + keyLen);
      System.out.println("Remaining: " + input.remaining());
      input.get(values, 0, keyLen);
      final String key = new String(values, 0, keyLen);
      System.out.println("Key found: " + key);
      final int valueLen = input.getInt();
      values = new byte[valueLen];
      System.out.println("ValueLen: " + valueLen);
      input.get(values, 0, valueLen);
//      if (fields == null || fields.contains(key)) { // Zamienic z funkcją map: retainAll
      result.put(key, new ByteArrayByteIterator(values, 0, valueLen));

    }

    return result;
  }
}

/**
 * A class that wraps the PmemKV to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class PmemKV extends DB {
  public static final String ENGINE_PROPERTY = "pmemkv.engine";
  public static final String SIZE_PROPERTY = "pmemkv.dbsize";
  public static final String PATH_PROPERTY = "pmemkv.dbpath";
  private static Database<byte[], Map<String, ByteIterator>> db = null;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String engineName = props.getProperty(ENGINE_PROPERTY);
    if (engineName == null) {
      engineName = "cmap";
    }
    String path = props.getProperty(PATH_PROPERTY);
    if (path == null) {
      throw new DBException(PATH_PROPERTY + " is obligatory to run");
    }
    String size = props.getProperty(SIZE_PROPERTY);
    if (size == null) {
      size = "1794967296";
    }
    boolean startError = false;
    try {
      db = new Database.Builder<byte[], Map<String, ByteIterator>>(engineName)
          .setSize(Integer.parseInt(size))
          .setPath(path)
          .setKeyConverter(new ByteConverter())
          .setValueConverter(new MapToByteBufferConverter())
          .build();
    } catch (DatabaseException e) {
      startError = true;
    }
    if (startError) {
      startError = false;
      try {
        db = new Database.Builder<byte[], Map<String, ByteIterator>>(engineName)
            .setSize(Integer.parseInt(size))
            .setPath(path)
            .setKeyConverter(new ByteConverter())
            .setValueConverter(new MapToByteBufferConverter())
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
      db.get(key.getBytes(UTF_8), (receivedMap) -> {
          result.putAll(receivedMap);
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
    Map<String, ByteIterator> currentValues = db.getCopy(key.getBytes(UTF_8));
    if (currentValues == null) {
      return Status.NOT_FOUND;
    }
//    //update
    result.putAll(values);

    //store
    db.put(key.getBytes(UTF_8), result);
    return Status.OK;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    db.put(key.getBytes(UTF_8), values);
    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      db.remove(key.getBytes(UTF_8));
    } catch (NotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }
}
