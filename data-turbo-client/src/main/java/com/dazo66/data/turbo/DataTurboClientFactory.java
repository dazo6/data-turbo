package com.dazo66.data.turbo;

import com.dazo66.data.turbo.model.AbstractFactory;
import com.dazo66.data.turbo.model.DataFileEnum;
import com.dazo66.data.turbo.model.DataTurboDetail;

/**
 * @author dazo66
 **/
public class DataTurboClientFactory extends AbstractFactory<String, DataTurboDetail,
        AbstractDataTurboClient> {

    private static final DataTurboClientFactory INSTANCE = new DataTurboClientFactory();

    static {
        INSTANCE.register(DataFileEnum.BLOOM.getId(), BloomDataTurboClient::new);
        INSTANCE.register(DataFileEnum.NORMAL.getId(), NormalDataTurboClient::new);
    }

    public static DataTurboClientFactory getInstance() {
        return INSTANCE;
    }

    public static AbstractDataTurboClient get(DataTurboDetail dataTurboDetail) {
        return getInstance().get(dataTurboDetail.getDataFileEnum().getId(), dataTurboDetail);
    }

    /**
     * 生成增量客户端 允许添加增量文件
     *
     * @param dataTurboDetail 需要补充的描述文件
     * @return 带补丁的客户端 会先查补丁进行返回
     */
    public PatchDataTurboClient getPatchClient(DataTurboDetail dataTurboDetail) {
        return new PatchDataTurboClient(get(dataTurboDetail.getDataFileEnum().getId(),
                dataTurboDetail));
    }
}
