package com.opendatasoft.elasticsearch.index.mapper.geo;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;

public class RegisterGeoType extends AbstractIndexComponent {

    @Inject
    protected RegisterGeoType(Index index, @IndexSettings Settings indexSettings, MapperService mapperService) {
        super(index, indexSettings);

        mapperService.documentMapperParser().putTypeParser("geo", new GeoMapper2.TypeParser());
    }
}
