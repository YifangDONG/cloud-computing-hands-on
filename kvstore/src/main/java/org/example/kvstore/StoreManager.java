package org.example.kvstore;


public class StoreManager {

    public static final String DEFAULT_STORE = "__luKVS";
        
    public <K,V> Store<K,V> newStore() {
        return newStore(DEFAULT_STORE);
    }

    public <K,V> Store<K,V> newStore(String name){
        try {
            StoreImpl<K,V> store = new StoreImpl<K, V>(name);
            store.init();
            return store;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public void stop(Store<?, ?> store) {
    	((StoreImpl<?, ?>) store).stop();
    }
}
