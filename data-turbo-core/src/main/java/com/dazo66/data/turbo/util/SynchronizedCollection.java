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
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author dazo66
 **/
public class SynchronizedCollection<E> implements Collection<E>, Serializable {
    final Collection<E> c;  // Backing Collection
    final Object mutex;     // Object on which to synchronize
    public SynchronizedCollection(Collection<E> c) {
        this.c = Objects.requireNonNull(c);
        mutex = this;
    }

    public SynchronizedCollection(Collection<E> c, Object mutex) {
        this.c = Objects.requireNonNull(c);
        this.mutex = Objects.requireNonNull(mutex);
    }

    @Override
    public int size() {
        synchronized (mutex) {
            return c.size();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (mutex) {
            return c.isEmpty();
        }
    }

    @Override
    public boolean contains(Object o) {
        synchronized (mutex) {
            return c.contains(o);
        }
    }

    @Override
    public Iterator<E> iterator() {
        return c.iterator(); // Must be manually synched by user!
    }

    @Override
    public Object[] toArray() {
        synchronized (mutex) {
            return c.toArray();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        synchronized (mutex) {
            return c.toArray(a);
        }
    }

    @Override
    public boolean add(E e) {
        synchronized (mutex) {
            return c.add(e);
        }
    }

    @Override
    public boolean remove(Object o) {
        synchronized (mutex) {
            return c.remove(o);
        }
    }

    @Override
    public boolean containsAll(Collection<?> coll) {
        synchronized (mutex) {
            return c.containsAll(coll);
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> coll) {
        synchronized (mutex) {
            return c.addAll(coll);
        }
    }

    @Override
    public boolean removeAll(Collection<?> coll) {
        synchronized (mutex) {
            return c.removeAll(coll);
        }
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        synchronized (mutex) {
            return c.removeIf(filter);
        }
    }

    @Override
    public boolean retainAll(Collection<?> coll) {
        synchronized (mutex) {
            return c.retainAll(coll);
        }
    }

    @Override
    public void clear() {
        synchronized (mutex) {
            c.clear();
        }
    }

    @Override
    public Spliterator<E> spliterator() {
        return c.spliterator(); // Must be manually synched by user!
    }

    @Override
    public Stream<E> stream() {
        return c.stream(); // Must be manually synched by user!
    }

    @Override
    public Stream<E> parallelStream() {
        return c.parallelStream(); // Must be manually synched by user!
    }

    @Override
    public String toString() {
        synchronized (mutex) {
            return c.toString();
        }
    }

    // Override default methods in Collection
    @Override
    public void forEach(Consumer<? super E> consumer) {
        synchronized (mutex) {
            c.forEach(consumer);
        }
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        synchronized (mutex) {
            s.defaultWriteObject();
        }
    }
    private static final long serialVersionUID = 3053995032091335093L;
}