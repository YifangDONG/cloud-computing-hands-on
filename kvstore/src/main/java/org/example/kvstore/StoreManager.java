package org.example.kvstore;

public class StoreManager {

	public static final String DEFAULT_STORE = "__xxKVS";
	private static final String DEFAULT_MODE = "CONSISTENTHASH";

	public <K, V> Store<K, V> newStore() {
		return newStore(DEFAULT_STORE, DEFAULT_MODE);
	}

	public <K, V> Store<K, V> newStore(String name) {
		try {
			StoreImpl<K, V> store = new StoreImpl<K, V>(name, DEFAULT_MODE);
			store.init();
			return store;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public <K, V> Store<K, V> newStore(String name, String mode) {
		try {
			StoreImpl<K, V> store = new StoreImpl<K, V>(name, mode);
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
