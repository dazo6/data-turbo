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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.function.Predicate;

import static com.dazo66.data.turbo.util.Preconditions.checkArgument;
import static com.dazo66.data.turbo.util.Preconditions.checkNotNull;


public class DiskBloomFilter<T extends Object> implements Predicate<T>, IBloomFilter<T>,
        Serializable {
    /**
     * The bit set of the BloomFilter (not necessarily power of 2!)
     */
    private final DiskBloomFilterStrategies.DiskBitArray diskBitArray;
    /**
     * Number of hashes per element
     */
    private final int numHashFunctions;
    /**
     * The funnel to translate Ts to bytes
     */
    private final Funnel<? super T> funnel;
    /**
     * The strategy we employ to map an element T to {@code numHashFunctions} bit indexes.
     */
    private final Strategy strategy;

    private DiskBloomFilter(DiskBloomFilterStrategies.DiskBitArray diskBitArray,
                            int numHashFunctions, Funnel<? super T> funnel, Strategy strategy) {
        checkArgument(numHashFunctions > 0, "numHashFunctions (%s) must be > 0", numHashFunctions);
        checkArgument(numHashFunctions <= 255, "numHashFunctions (%s) must be <= 255",
                numHashFunctions);
        this.diskBitArray = checkNotNull(diskBitArray);
        this.numHashFunctions = numHashFunctions;
        this.funnel = checkNotNull(funnel);
        this.strategy = checkNotNull(strategy);
    }

    /**
     * Creates a {@link DiskBloomFilter} with the expected number of insertions and expected false
     * positive probability.
     *
     * <p>Note that overflowing a {@code BloomFilter} with significantly more elements than
     * specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * <p>The constructed {@code BloomFilter} will be serializable if the provided {@code Funnel<T>}
     * is.
     *
     * <p>It is recommended that the funnel be implemented as a Java enum. This has the benefit of
     * ensuring proper serialization and deserialization, which is important since {@link #equals}
     * also relies on object identity of funnels.
     *
     * @param funnel             the funnel of T's that the constructed {@code BloomFilter} will use
     * @param expectedInsertions the number of expected insertions to the constructed {@code
     *                           BloomFilter}; must be positive
     * @param fpp                the desired false positive probability (must be positive and
     *                           less than 1.0)
     * @return a {@code BloomFilter}
     */
    public static <T extends Object> DiskBloomFilter<T> create(Funnel<? super T> funnel,
                                                               int expectedInsertions, double fpp) {
        return create(funnel, (long) expectedInsertions, fpp);
    }

    /**
     * Creates a {@link DiskBloomFilter} with the expected number of insertions and expected false
     * positive probability.
     *
     * <p>Note that overflowing a {@code BloomFilter} with significantly more elements than
     * specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * <p>The constructed {@code BloomFilter} will be serializable if the provided {@code Funnel<T>}
     * is.
     *
     * <p>It is recommended that the funnel be implemented as a Java enum. This has the benefit of
     * ensuring proper serialization and deserialization, which is important since {@link #equals}
     * also relies on object identity of funnels.
     *
     * @param funnel             the funnel of T's that the constructed {@code BloomFilter} will use
     * @param expectedInsertions the number of expected insertions to the constructed {@code
     *                           BloomFilter}; must be positive
     * @param fpp                the desired false positive probability (must be positive and
     *                           less than 1.0)
     * @return a {@code BloomFilter}
     * @since 19.0
     */
    public static <T extends Object> DiskBloomFilter<T> create(Funnel<? super T> funnel,
                                                               long expectedInsertions,
                                                               double fpp) {
        return create(funnel, expectedInsertions, fpp, DiskBloomFilterStrategies.MURMUR128_MITZ_64);
    }

    static <T extends Object> DiskBloomFilter<T> create(Funnel<? super T> funnel,
                                                        long expectedInsertions, double fpp,
                                                        Strategy strategy) {
        checkNotNull(funnel);
        checkArgument(expectedInsertions >= 0, "Expected insertions (%s) must be >= 0",
                expectedInsertions);
        checkArgument(fpp > 0.0, "False positive probability (%s) must be > 0.0", fpp);
        checkArgument(fpp < 1.0, "False positive probability (%s) must be < 1.0", fpp);
        checkNotNull(strategy);

        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }
        long numBits = optimalNumOfBits(expectedInsertions, fpp);
        int numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
        try {
            return new DiskBloomFilter<T>(new DiskBloomFilterStrategies.DiskBitArray(numBits),
                    numHashFunctions, funnel, strategy);
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalArgumentException("Could not create BloomFilter of " + numBits + " " + "bits", e);
        }
    }

    /**
     * Creates a {@link DiskBloomFilter} with the expected number of insertions and a default
     * expected
     * false positive probability of 3%.
     *
     * <p>Note that overflowing a {@code BloomFilter} with significantly more elements than
     * specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * <p>The constructed {@code BloomFilter} will be serializable if the provided {@code Funnel<T>}
     * is.
     *
     * <p>It is recommended that the funnel be implemented as a Java enum. This has the benefit of
     * ensuring proper serialization and deserialization, which is important since {@link #equals}
     * also relies on object identity of funnels.
     *
     * @param funnel             the funnel of T's that the constructed {@code BloomFilter} will use
     * @param expectedInsertions the number of expected insertions to the constructed {@code
     *                           BloomFilter}; must be positive
     * @return a {@code BloomFilter}
     */
    public static <T extends Object> DiskBloomFilter<T> create(Funnel<? super T> funnel,
                                                               int expectedInsertions) {
        return create(funnel, (long) expectedInsertions);
    }

    /**
     * Creates a {@link DiskBloomFilter} with the expected number of insertions and a default
     * expected
     * false positive probability of 3%.
     *
     * <p>Note that overflowing a {@code BloomFilter} with significantly more elements than
     * specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * <p>The constructed {@code BloomFilter} will be serializable if the provided {@code Funnel<T>}
     * is.
     *
     * <p>It is recommended that the funnel be implemented as a Java enum. This has the benefit of
     * ensuring proper serialization and deserialization, which is important since {@link #equals}
     * also relies on object identity of funnels.
     *
     * @param funnel             the funnel of T's that the constructed {@code BloomFilter} will use
     * @param expectedInsertions the number of expected insertions to the constructed {@code
     *                           BloomFilter}; must be positive
     * @return a {@code BloomFilter}
     * @since 19.0
     */
    public static <T extends Object> DiskBloomFilter<T> create(Funnel<? super T> funnel,
                                                               long expectedInsertions) {
        return create(funnel, expectedInsertions, 0.03); // FYI, for 3%, we always get 5 hash
        // functions
    }

    /**
     * Computes the optimal k (number of hashes per element inserted in Bloom filter), given the
     * expected insertions and total number of bits in the Bloom filter.
     *
     * <p>See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
     *
     * @param n expected insertions (must be positive)
     * @param m total number of bits in Bloom filter (must be positive)
     */
    static int optimalNumOfHashFunctions(long n, long m) {
        // (m / n) * log(2), but avoid truncation due to division!
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * Computes m (total bits of Bloom filter) which is expected to achieve, for the specified
     * expected insertions, the required false positive probability.
     *
     * <p>See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the
     * formula.
     *
     * @param n expected insertions (must be positive)
     * @param p false positive rate (must be 0 < p < 1)
     */
    static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * Reads a byte stream, which was written by {@linkplain #writeTo(OutputStream)}, into a {@code
     * BloomFilter}.
     *
     * <p>The {@code Funnel} to be used is not encoded in the stream, so it must be provided here.
     * <b>Warning:</b> the funnel provided <b>must</b> behave identically to the one used to
     * populate
     * the original Bloom filter!
     *
     * @throws IOException if the InputStream throws an {@code IOException}, or if its data does not
     *                     appear to be a BloomFilter serialized using the
     *                     {@linkplain #writeTo(OutputStream)} method.
     */
    public static <T extends Object> DiskBloomFilter<T> readFrom(File file,
                                                                 Funnel<? super T> funnel) throws IOException {
        checkNotNull(file, "InputStream");
        checkNotNull(funnel, "Funnel");
        int strategyOrdinal = -1;
        int numHashFunctions = -1;
        try {
            FileInputStream din = new FileInputStream(file);
            strategyOrdinal = din.read();
            numHashFunctions = din.read();
            din.close();

            Strategy strategy = DiskBloomFilterStrategies.values()[strategyOrdinal];
            return new DiskBloomFilter<>(new DiskBloomFilterStrategies.DiskBitArray(file),
                    numHashFunctions, funnel, strategy);
        } catch (Exception e) {
            String message = "Unable to deserialize BloomFilter from InputStream." + " " +
                    "strategyOrdinal: " + strategyOrdinal + " numHashFunctions: " + numHashFunctions + " dataLength: ";
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public boolean test(T t) {
        return apply(t);
    }

    /**
     * Creates a new {@code BloomFilter} that's a copy of this instance. The new instance is
     * equal to
     * this instance but shares no mutable state.
     *
     * @since 12.0
     */
    public DiskBloomFilter<T> copy() {
        return new DiskBloomFilter<T>(diskBitArray, numHashFunctions, funnel, strategy);
    }

    /**
     * Returns {@code true} if the element <i>might</i> have been put in this Bloom filter, {@code
     * false} if this is <i>definitely</i> not the case.
     */
    public boolean mightContain(T object) {
        return strategy.mightContain(object, funnel, numHashFunctions, diskBitArray);
    }

    /**
     * @deprecated Provided only to satisfy the {@link Predicate} interface; use
     * {@link #mightContain}
     * instead.
     */
    @Deprecated
    public boolean apply(T input) {
        return mightContain(input);
    }

    /**
     * Puts an element into this {@code BloomFilter}. Ensures that subsequent invocations of {@link
     * #mightContain(Object)} with the same element will always return {@code true}.
     *
     * @return true if the Bloom filter's bits changed as a result of this operation. If the bits
     * changed, this is <i>definitely</i> the first time {@code object} has been added to the
     * filter. If the bits haven't changed, this <i>might</i> be the first time {@code object} has
     * been added to the filter. Note that {@code put(t)} always returns the <i>opposite</i>
     * result to what {@code mightContain(t)} would have returned at the time it is called.
     * @since 12.0 (present in 11.0 with {@code void} return type})
     */
    @Override
    public boolean put(T object) {
        return strategy.put(object, funnel, numHashFunctions, diskBitArray);
    }

    /**
     * Writes this {@code BloomFilter} to an output stream, with a custom format (not Java
     * serialization). This has been measured to save at least 400 bytes compared to regular
     * serialization.
     *
     * <p>Use {@linkplain #readFrom(File, Funnel)} to reconstruct the written BloomFilter.
     */
    @Override
    public void writeTo(OutputStream out) throws Exception {
        diskBitArray.fileChannelPool.flush();
        // Serial form:
        // 1 signed byte for the strategy
        // 1 unsigned byte for the number of hash functions
        // 1 big endian long, the number of longs in our bitset
        // N big endian longs of our bitset
        DataOutputStream dout = new DataOutputStream(out);
        dout.writeByte(ByteUtils.checkedCast(strategy.ordinal()));
        dout.writeByte(ByteUtils.unsignedCheckedCast(numHashFunctions)); // note: checked at the
        // c'tor
        dout.writeLong(diskBitArray.bitSize());
        FileChannel fileChannel = diskBitArray.fileChannelPool.getFileChannel();
        int offset = DiskBloomFilterStrategies.DiskBitArray.FILE_OFFSET;
        fileChannel.position(offset / 8);
        ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
        while (true) {
            int count = fileChannel.read(byteBuffer);
            if (count <= -1) {
                break;
            }
            byteBuffer.flip();
            while (byteBuffer.hasRemaining()) {
                out.write(byteBuffer.get());
            }
            byteBuffer.compact();
        }
        diskBitArray.fileChannelPool.putFileChannel(fileChannel);
        out.flush();
    }

    /**
     * Returns the probability that {@linkplain #mightContain(Object)} will erroneously return
     * {@code
     * true} for an object that has not actually been put in the {@code BloomFilter}.
     *
     * <p>Ideally, this number should be close to the {@code fpp} parameter passed in {@linkplain
     * #create(Funnel, int, double)}, or smaller. If it is significantly higher, it is usually the
     * case that too many elements (more than expected) have been put in the {@code BloomFilter},
     * degenerating it.
     *
     * @since 14.0 (since 11.0 as expectedFalsePositiveProbability())
     */
    public double expectedFpp() {
        return Math.pow((double) diskBitArray.bitCount() / bitSize(), numHashFunctions);
    }

    /**
     * Returns an estimate for the total number of distinct elements that have been added to this
     * Bloom filter. This approximation is reasonably accurate if it does not exceed the value of
     * {@code expectedInsertions} that was used when constructing the filter.
     *
     * @since 22.0
     */
    public long approximateElementCount() {
        try {
            long bitSize = diskBitArray.bitSize();
            long bitCount = diskBitArray.bitCount();

            /**
             * Each insertion is expected to reduce the # of clear bits by a factor of
             * `numHashFunctions/bitSize`. So, after n insertions, expected bitCount is `bitSize
             * * (1 - (1 -
             * numHashFunctions/bitSize)^n)`. Solving that for n, and approximating `ln x` as `x
             * - 1` when x
             * is close to 1 (why?), gives the following formula.
             */
            double fractionOfBitsSet = (double) bitCount / bitSize;
            return DoubleMath.roundToLong(-Math.log1p(-fractionOfBitsSet) * bitSize / numHashFunctions, RoundingMode.HALF_UP);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of bits in the underlying bit array.
     */
    long bitSize() {
        try {
            return diskBitArray.bitSize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Cheat sheet:
    //
    // m: total bits
    // n: expected insertions
    // b: m/n, bits per insertion
    // p: expected false positive probability
    //
    // 1) Optimal k = b * ln2
    // 2) p = (1 - e ^ (-kn/m))^k
    // 3) For optimal k: p = 2 ^ (-k) ~= 0.6185^b
    // 4) For optimal k: m = -nlnp / ((ln2) ^ 2)

    /**
     * Determines whether a given Bloom filter is compatible with this Bloom filter. For two Bloom
     * filters to be compatible, they must:
     *
     * <ul>
     *   <li>not be the same instance
     *   <li>have the same number of hash functions
     *   <li>have the same bit size
     *   <li>have the same strategy
     *   <li>have equal funnels
     * </ul>
     *
     * @param that The Bloom filter to check for compatibility.
     * @since 15.0
     */
    public boolean isCompatible(DiskBloomFilter<T> that) {
        checkNotNull(that);
        return this != that && this.numHashFunctions == that.numHashFunctions && this.bitSize() == that.bitSize() && this.strategy.equals(that.strategy) && this.funnel.equals(that.funnel);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{numHashFunctions, funnel, strategy, diskBitArray});
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof DiskBloomFilter) {
            DiskBloomFilter<?> that = (DiskBloomFilter<?>) object;
            return this.numHashFunctions == that.numHashFunctions && this.funnel.equals(that.funnel) && this.diskBitArray.equals(that.diskBitArray) && this.strategy.equals(that.strategy);
        }
        return false;
    }

    private Object writeReplace() {
        return new SerialForm<T>(this);
    }

    @Override
    public void close() throws IOException {
        diskBitArray.file.delete();
        try {
            diskBitArray.fileChannelPool.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * A strategy to translate T instances, to {@code numHashFunctions} bit indexes.
     *
     * <p>Implementations should be collections of pure functions (i.e. stateless).
     */
    interface Strategy extends Serializable {

        /**
         * Sets {@code numHashFunctions} bits of the given bit array, by hashing a user element.
         *
         * <p>Returns whether any bits changed as a result of this operation.
         */
        <T extends Object> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions,
                                       DiskBloomFilterStrategies.DiskBitArray diskBitArray);

        /**
         * Queries {@code numHashFunctions} bits of the given bit array, by hashing a user element;
         * returns {@code true} if and only if all selected bits are set.
         */
        <T extends Object> boolean mightContain(T object, Funnel<? super T> funnel,
                                                int numHashFunctions,
                                                DiskBloomFilterStrategies.DiskBitArray diskBitArray);

        /**
         * Identifier used to encode this strategy, when marshalled as part of a BloomFilter. Only
         * values in the [-128, 127] range are valid for the compact serial form. Non-negative
         * values
         * are reserved for enums defined in BloomFilterStrategies; negative values are reserved
         * for any
         * custom, stateful strategy we may define (e.g. any kind of strategy that would depend
         * on user
         * input).
         */
        int ordinal();
    }

    private static class SerialForm<T extends Object> implements Serializable {
        private static final long serialVersionUID = 1;
        final File file;
        final int numHashFunctions;
        final Funnel<? super T> funnel;
        final Strategy strategy;

        SerialForm(DiskBloomFilter<T> bf) {
            this.file = bf.diskBitArray.file;
            this.numHashFunctions = bf.numHashFunctions;
            this.funnel = bf.funnel;
            this.strategy = bf.strategy;
        }

        Object readResolve() {
            try {
                return new DiskBloomFilter<T>(new DiskBloomFilterStrategies.DiskBitArray(file),
                        numHashFunctions, funnel, strategy);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}