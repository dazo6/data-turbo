/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.dazo66.data.turbo.util;

import java.io.*;
import java.nio.channels.FileChannel;

import static com.dazo66.data.turbo.util.Murmur3_128HashFunction.MURMUR3_128;
import static com.dazo66.data.turbo.util.Preconditions.checkArgument;

public enum DiskBloomFilterStrategies implements DiskBloomFilter.Strategy {

    /**
     * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and
     * Michael
     * Mitzenmacher. The paper argues that this trick doesn't significantly deteriorate the
     * performance of a Bloom filter (yet only needs two 32bit hash functions).
     */
    MURMUR128_MITZ_32() {
        @Override
        public <T extends Object> boolean put(T object, Funnel<? super T> funnel,
                                              int numHashFunctions,
                                              DiskBloomFilterStrategies.DiskBitArray bits) {
            try {
                long bitSize = bits.bitSize();
                long hash64 = MURMUR3_128.hashObject(object, funnel).asLong();
                int hash1 = (int) hash64;
                int hash2 = (int) (hash64 >>> 32);

                boolean bitsChanged = false;
                for (int i = 1; i <= numHashFunctions; i++) {
                    int combinedHash = hash1 + (i * hash2);
                    // Flip all the bits if it's negative (guaranteed positive number)
                    if (combinedHash < 0) {
                        combinedHash = ~combinedHash;
                    }
                    bitsChanged |= bits.set(combinedHash % bitSize);
                }
                return bitsChanged;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public <T extends Object> boolean mightContain(T object, Funnel<? super T> funnel,
                                                       int numHashFunctions,
                                                       DiskBloomFilterStrategies.DiskBitArray bits) {
            try {
                long bitSize = bits.bitSize();
                long hash64 = MURMUR3_128.hashObject(object, funnel).asLong();
                int hash1 = (int) hash64;
                int hash2 = (int) (hash64 >>> 32);

                for (int i = 1; i <= numHashFunctions; i++) {
                    int combinedHash = hash1 + (i * hash2);
                    // Flip all the bits if it's negative (guaranteed positive number)
                    if (combinedHash < 0) {
                        combinedHash = ~combinedHash;
                    }
                    if (!bits.get(combinedHash % bitSize)) {
                        return false;
                    }
                }
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    },

    MURMUR128_MITZ_64() {
        @Override
        public <T extends Object> boolean put(T object, Funnel<? super T> funnel,
                                              int numHashFunctions,
                                              DiskBloomFilterStrategies.DiskBitArray bits) {
            try {
                long bitSize = bits.bitSize();
                byte[] bytes = MURMUR3_128.hashObject(object, funnel).getBytesInternal();
                long hash1 = lowerEight(bytes);
                long hash2 = upperEight(bytes);

                boolean bitsChanged = false;
                long combinedHash = hash1;
                for (int i = 0; i < numHashFunctions; i++) {
                    // Make the combined hash positive and indexable
                    bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
                    combinedHash += hash2;
                }
                return bitsChanged;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public <T extends Object> boolean mightContain(T object, Funnel<? super T> funnel,
                                                       int numHashFunctions,
                                                       DiskBloomFilterStrategies.DiskBitArray bits) {
            try {
                long bitSize = bits.bitSize();
                byte[] bytes = MURMUR3_128.hashObject(object, funnel).getBytesInternal();
                long hash1 = lowerEight(bytes);
                long hash2 = upperEight(bytes);

                long combinedHash = hash1;
                for (int i = 0; i < numHashFunctions; i++) {
                    // Make the combined hash positive and indexable
                    if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
                        return false;
                    }
                    combinedHash += hash2;
                }
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private /* static */ long lowerEight(byte[] bytes) {
            return Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2],
                    bytes[1], bytes[0]);
        }

        private /* static */ long upperEight(byte[] bytes) {
            return Longs.fromBytes(bytes[15], bytes[14], bytes[13], bytes[12], bytes[11],
                    bytes[10], bytes[9], bytes[8]);
        }
    };

    /**
     * Models a disk of bits.
     *
     * <p>We use this instead of java.util.BitSet because we need access to the array of longs
     * and we
     * need compare-and-swap.
     */
    static final class DiskBitArray {
        File file;
        FileChannelPool fileChannelPool;
        private LongAddable bitCount;
        DiskBitArray(long numBits) throws IOException {
            String usrHome = System.getProperty("user.home");
            checkArgument(numBits > 0, "num of bits must be positive!");
            checkArgument(!StringUtils.isEmpty(usrHome), "user home must not empty");
            File tempFile = new File(usrHome + String.format("/data-turbo/temp-%d.bloom",
                    System.currentTimeMillis()));
            File parentFile = tempFile.getParentFile();
            if (parentFile != null) {
                parentFile.mkdirs();
            }
            FileOutputStream stream = new FileOutputStream(tempFile);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(stream);
            numBits += 64 - (numBits % 64);
            long l = (numBits + FILE_OFFSET) >>> 3;
            for (long i = 0; i < l; i++) {
                bufferedOutputStream.write(0);
            }
            bufferedOutputStream.close();
            init(tempFile);
        }

        DiskBitArray(File file) throws IOException {
            checkArgument(file.exists(), "file must exist!");
            init(file);
        }

        private void init(File file) {
            this.file = file;
            this.bitCount = LongAddables.create();
            try {
                fileChannelPool = new FileChannelPool(file);
            } catch (FileNotFoundException e) {
                // ignore
            }
        }

        /**
         * Returns true if the bit changed value.
         */
        boolean set(long bitIndex) {
            long offset = bitIndex + FILE_OFFSET;
            FileChannel fileChannel = fileChannelPool.getFileChannel();
            try {
                if (IOUtils.getOneBit(fileChannel, offset)) {
                    return false;
                }
                fileChannelPool.writeOneBit(offset);
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                fileChannelPool.putFileChannel(fileChannel);
            }
        }

        boolean get(long bitIndex) {
            FileChannel fileChannel = fileChannelPool.getFileChannel();
            try {
                return IOUtils.getOneBit(fileChannel, bitIndex + FILE_OFFSET);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                fileChannelPool.putFileChannel(fileChannel);
            }
        }

        /**
         * Number of bits
         */
        long bitSize() throws IOException {
            FileChannel fileChannel = fileChannelPool.getFileChannel();
            try {
                return fileChannel.size() * 8 - FILE_OFFSET;
            } finally {
                fileChannelPool.putFileChannel(fileChannel);
            }
        }

        /**
         * Number of set bits (1s).
         *
         * <p>Note that because of concurrent set calls and uses of atomics, this bitCount is a
         * (very)
         * close *estimate* of the actual number of bits set. It's not possible to do better than an
         * estimate without locking. Note that the number, if not exactly accurate, is *always*
         * underestimating, never overestimating.
         */
        long bitCount() {
            return bitCount.sum();
        }

        @Override
        public int hashCode() {
            return file.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof DiskBitArray) {
                return ((DiskBitArray) o).file.equals(file);
            }
            return false;
        }
        public static final int FILE_OFFSET = 10 * 8;
    }

}
