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

import static com.dazo66.data.turbo.util.Preconditions.checkArgument;

/**
 * @author dazo66
 **/
public class ByteUtils {

    private static final int UNSIGNED_MASK = 0xFF;
    private static final int b0 = Integer.valueOf("00000000", 2);
    private static final int b1 = Integer.valueOf("10000000", 2);
    private static final int b2 = Integer.valueOf("11000000", 2);
    private static final int b3 = Integer.valueOf("11100000", 2);
    private static final int b4 = Integer.valueOf("11110000", 2);

    /**
     * Returns the value of the given byte as an integer, when treated as unsigned. That is, returns
     * {@code value + 256} if {@code value} is negative; {@code value} itself otherwise.
     *
     * <p><b>Java 8 users:</b> use {@link Byte#toUnsignedInt(byte)} instead.
     *
     * @since 6.0
     */
    public static int toIntIgnoreSign(byte value) {
        return value & UNSIGNED_MASK;
    }

    public static byte checkedCast(long value) {
        byte result = (byte) value;
        checkArgument(result == value, "Out of range: %s", value);
        return result;
    }

    /**
     * Returns the {@code byte} value that, when treated as unsigned, is equal to {@code value}, if
     * possible.
     *
     * @param value a value between 0 and 255 inclusive
     * @return the {@code byte} value that, when treated as unsigned, equals {@code value}
     * @throws IllegalArgumentException if {@code value} is negative or greater than 255
     */
    public static byte unsignedCheckedCast(long value) {
        checkArgument(value >> Byte.SIZE == 0, "out of range: %s", value);
        return (byte) value;
    }

    public static Pair<Integer, Integer> getUnbrokenUTF8ArrayRange(byte[] bytes) {
        int offset = 0;
        boolean isStart = false;
        for (int i = 0; i < bytes.length; i++) {
            int i1 = toIntIgnoreSign(bytes[i]);
            if (!isStart) {
                if (i1 >= b1 && i1 < b2) {
                    offset++;
                } else {
                    isStart = true;
                }
            } else {
                if (i1 >= b4) {
                    if (i + 4 < bytes.length) {
                        i += 3;
                    } else {
                        return Pair.of(offset, i - offset);
                    }
                } else if (i1 >= b3) {
                    if (i + 3 < bytes.length) {
                        i += 2;
                    } else {
                        return Pair.of(offset, i - offset);
                    }
                } else if (i1 >= b2) {
                    if (i + 2 < bytes.length) {
                        i += 1;
                    } else {
                        return Pair.of(offset, i - offset);
                    }
                }
            }
        }
        return Pair.of(offset, bytes.length - offset);
    }

    /**
     * 计算给定的字符串在utf8编码下的byte长度
     *
     * @param src 给定的字符串
     * @return int
     */
    public static int getByteCountUTF8(String src) {
        if (StringUtils.isEmpty(src)) {
            return 0;
        }
        int ret = 0;
        for (int i = 0; i < src.length(); i++) {
            char c = src.charAt(i);
            if (c < '\u0080') {
                ret += 1;
            } else if (c < '\u0800') {
                ret += 2;
            } else {
                ret += 3;
            }
        }
        return ret;
    }

    /**
     * 返回target在src中第一个出现的序号 包括target
     *
     * @param src    原始数组
     * @param target 搜索数组
     * @return
     */
    public static int indexOfBytes(byte[] src, byte[] target, int offset) {
        Preconditions.checkArgument(offset < src.length);
        out:
        for (int i1 = offset; i1 < src.length; i1++) {
            byte b1 = src[i1];
            for (byte b2 : target) {
                if (b1 != b2) {
                    continue out;
                }
            }
            return i1;
        }
        return -1;
    }

}
