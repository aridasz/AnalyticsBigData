package org.am.cdo.data.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("dataStoreFactory")
public class DataStoreFactory {

	@Autowired
    private List<IDataStore> dataStores;

    private static final Map<String, IDataStore> dataStoreCache = new HashMap<>();

    @PostConstruct
    public void initDataStoreCache() {
        for(IDataStore dataStore : dataStores) {
        	dataStoreCache.put(dataStore.getClass().getSimpleName(), dataStore);
        }
    }

    public static IDataStore getDataStore(String type) {
    	IDataStore dataStore = dataStoreCache.get(type);
        if(dataStore == null) 
        	throw new RuntimeException("Unknown service type: " + type);
        return dataStore;
    }
}
