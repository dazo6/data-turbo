package com.dazo66.data.turbo;

import com.dazo66.data.turbo.key.predictor.IKeyComparator;
import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.DataTurboResult;
import com.dazo66.data.turbo.model.LoadEnum;
import com.dazo66.data.turbo.util.DiskBloomFilter;
import com.dazo66.data.turbo.util.Funnels;
import com.dazo66.data.turbo.util.HeapBloomFilter;
import com.dazo66.data.turbo.util.MemoryUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * 布隆过滤器客户端
 *
 * @author dazo66
 **/
public class BloomDataTurboClient extends AbstractDataTurboClient {


    private final AtomicReference<Predicate<CharSequence>> bloomFilter = new AtomicReference<>();
    private final ReentrantLock lock = new ReentrantLock();


    public BloomDataTurboClient(DataTurboDetail dataTurboDetail) {
        super(dataTurboDetail);
    }

    @Override
    public DataTurboResult search(String key) {
        if (searchNameList(key)) {
            return new DataTurboResult(key, null);
        } else {
            return null;
        }
    }

    @Override
    public boolean searchNameList(String key) {
        if (bloomFilter.get() == null) {
            throw new RuntimeException("bloom data did not init!");
        } else {
            return bloomFilter.get().test(key);
        }
    }

    @Override
    public void load() throws Exception {
        lock.lock();
        try (BufferedInputStream bufferedOutputStream = new BufferedInputStream(Files.newInputStream(Paths.get(getDataTurboDetail().getDataFile())))) {
            LoadEnum loadEnum = getDataTurboDetail().getLoadEnum();
            Predicate<CharSequence> bloomFilter = null;
            if (loadEnum == LoadEnum.HEAP) {
                int available = bufferedOutputStream.available();
                if (MemoryUtils.checkFreeMemory(available)) {
                    bloomFilter = HeapBloomFilter.readFrom(bufferedOutputStream,
                            Funnels.stringFunnel(StandardCharsets.UTF_8));
                } else {
                    loadEnum = LoadEnum.DISK;
                }
            }
            if (loadEnum == LoadEnum.DISK) {
                bloomFilter =
                        DiskBloomFilter.readFrom(new File(getDataTurboDetail().getDataFile()),
                                Funnels.stringFunnel(StandardCharsets.UTF_8));
            }
            this.bloomFilter.set(bloomFilter);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            bloomFilter.set(null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IKeyComparator getComparator() {
        return null;
    }
}
