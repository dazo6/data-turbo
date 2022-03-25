package com.dazo66.data.turbo;

import com.dazo66.data.turbo.model.AbstractFactory;
import com.dazo66.data.turbo.model.DataFileEnum;
import com.dazo66.data.turbo.model.DataTurboDetail;

/**
 * @author dazo66
 **/
public class DataTurboBuilderFactory extends AbstractFactory<String, DataTurboDetail,
        AbstractDataTurboBuilder> {

    private static DataTurboBuilderFactory instance = new DataTurboBuilderFactory();

    static {
        instance.register(DataFileEnum.BLOOM.getId(), BloomDataTurboBuilder::new);
        instance.register(DataFileEnum.NORMAL.getId(), NormalDataTurboBuilder::new);
    }

    public static DataTurboBuilderFactory getInstance() {
        return instance;
    }

    public static AbstractDataTurboBuilder get(DataTurboDetail dataTurboDetail) {
        return getInstance().get(dataTurboDetail.getDataFileEnum().getId(), dataTurboDetail);
    }
}
