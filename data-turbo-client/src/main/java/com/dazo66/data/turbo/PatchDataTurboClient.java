package com.dazo66.data.turbo;

import com.dazo66.data.turbo.key.predictor.IKeyComparator;
import com.dazo66.data.turbo.model.DataTurboDetail;
import com.dazo66.data.turbo.model.DataTurboResult;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 支持增量的客户端
 * 其中名单查询不能修改之前的数据 因为只有true or false
 *
 * @author dazo66
 **/
public class PatchDataTurboClient extends AbstractDataTurboClient {

    private final AbstractDataTurboClient abstractDataTurboClient;

    private final Set<AbstractDataTurboClient> patches =
            Collections.synchronizedSet(new LinkedHashSet<>());

    public PatchDataTurboClient(AbstractDataTurboClient abstractDataTurboClient) {
        super(abstractDataTurboClient.getDataTurboDetail());
        this.abstractDataTurboClient = abstractDataTurboClient;
    }

    @Override
    public DataTurboResult search(String key) {
        for (AbstractDataTurboClient client : patches) {
            DataTurboResult result = client.search(key);
            if (result != null) {
                return result;
            }
        }
        return abstractDataTurboClient.search(key);
    }

    @Override
    public boolean searchNameList(String key) {
        for (AbstractDataTurboClient client : patches) {
            if (client.searchNameList(key)) {
                return true;
            }
        }
        return abstractDataTurboClient.searchNameList(key);
    }

    @Override
    public void load() throws Exception {
        abstractDataTurboClient.load();
        for (AbstractDataTurboClient client : patches) {
            client.load();
        }
    }

    @Override
    public void reload(DataTurboDetail dataTurboDetail) throws Exception {
        abstractDataTurboClient.reload(dataTurboDetail);
        for (AbstractDataTurboClient client : patches) {
            client.load();
        }
    }

    @Override
    public void close() throws Exception {
        abstractDataTurboClient.close();
        for (AbstractDataTurboClient client : patches) {
            client.close();
        }
    }

    @Override
    public IKeyComparator getComparator() {
        return null;
    }

    /**
     * 统一的添加增量的方法 使用了增量之后就可以父
     *
     * @param dataTurboDetail 添加增量的描述文件
     */
    public void addPatch(DataTurboDetail dataTurboDetail) throws Exception {
        AbstractDataTurboClient client =
                DataTurboClientFactory.getInstance().get(dataTurboDetail.getDataFileEnum().getId(), dataTurboDetail);
        if (client != null) {
            client.load();
            patches.add(client);
        } else {
            throw new RuntimeException("build patches client with error");
        }
    }
}
