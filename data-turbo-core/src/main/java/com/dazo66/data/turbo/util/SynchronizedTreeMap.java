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

/*
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
 */

/*
 * Source:
 * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
 * (Modified to adapt to Guava coding conventions and to use the HashFunction interface)
 */

package com.dazo66.data.turbo.util;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author dazo66
 **/
public class SynchronizedTreeMap<K, V> extends TreeMap<K, V> implements Serializable {

    final Object mutex;        // Object on which to synchronize
    private final TreeMap<K, V> m;     // Backing Map
    private transient Set<K> keySet;
    private transient Set<Map.Entry<K, V>> entrySet;
    private transient Collection<V> values;
    public SynchronizedTreeMap(TreeMap<K, V> m) {
        this.m = Objects.requireNonNull(m);
        mutex = this;
    }

    public SynchronizedTreeMap(TreeMap<K, V> m, Object mutex) {
        this.m = m;
        this.mutex = mutex;
    }

    @Override
    public int size() {
        synchronized (mutex) {
            return m.size();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        synchronized (mutex) {
            return m.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        synchronized (mutex) {
            return m.containsValue(value);
        }
    }

    @Override
    public V get(Object key) {
        synchronized (mutex) {
            return m.get(key);
        }
    }

    @Override
    public K firstKey() {
        synchronized (mutex) {
            return m.firstKey();
        }
    }

    @Override
    public K lastKey() {
        synchronized (mutex) {
            return m.lastKey();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        synchronized (mutex) {
            m.putAll(map);
        }
    }

    @Override
    public V put(K key, V value) {
        synchronized (mutex) {
            return m.put(key, value);
        }
    }

    @Override
    public V remove(Object key) {
        synchronized (mutex) {
            return m.remove(key);
        }
    }

    @Override
    public void clear() {
        synchronized (mutex) {
            m.clear();
        }
    }

    @Override
    public Set<K> keySet() {
        synchronized (mutex) {
            if (keySet == null) {
                keySet = new SynchronizedSet<>(m.keySet(), mutex);
            }
            return keySet;
        }
    }

    @Override
    public Collection<V> values() {
        synchronized (mutex) {
            if (values == null) {
                values = new SynchronizedCollection<>(m.values(), mutex);
            }
            return values;
        }
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        synchronized (mutex) {
            if (entrySet == null) {
                entrySet = new SynchronizedSet<>(m.entrySet(), mutex);
            }
            return entrySet;
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        synchronized (mutex) {
            return m.replace(key, oldValue, newValue);
        }
    }

    @Override
    public V replace(K key, V value) {
        synchronized (mutex) {
            return m.replace(key, value);
        }
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        synchronized (mutex) {
            m.forEach(action);
        }
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        synchronized (mutex) {
            m.replaceAll(function);
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (mutex) {
            return m.isEmpty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        synchronized (mutex) {
            return m.equals(o);
        }
    }

    @Override
    public int hashCode() {
        synchronized (mutex) {
            return m.hashCode();
        }
    }

    @Override
    public String toString() {
        synchronized (mutex) {
            return m.toString();
        }
    }

    // Override default methods in Map
    @Override
    public V getOrDefault(Object k, V defaultValue) {
        synchronized (mutex) {
            return m.getOrDefault(k, defaultValue);
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        synchronized (mutex) {
            return m.putIfAbsent(key, value);
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        synchronized (mutex) {
            return m.remove(key, value);
        }
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        synchronized (mutex) {
            return m.computeIfAbsent(key, mappingFunction);
        }
    }

    @Override
    public V computeIfPresent(K key,
                              BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        synchronized (mutex) {
            return m.computeIfPresent(key, remappingFunction);
        }
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        synchronized (mutex) {
            return m.compute(key, remappingFunction);
        }
    }

    @Override
    public V merge(K key, V value,
                   BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        synchronized (mutex) {
            return m.merge(key, value, remappingFunction);
        }
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        synchronized (mutex) {
            s.defaultWriteObject();
        }
    }
    private static final long serialVersionUID = 1978198479659022715L;

}
