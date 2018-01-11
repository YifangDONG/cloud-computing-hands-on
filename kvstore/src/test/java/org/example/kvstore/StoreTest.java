package org.example.kvstore;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class StoreTest {

//    @Test
//    public void baseOperations() {
//        StoreManager manager = new StoreManager();
//        Store<Integer, Integer> store = manager.newStore();
//        assert store.get(1) == null;
//
//        store.put(42, 1);
//        assert store.get(42).equals(1);
//        assert store.put(42, 2).equals(1);
//        manager.stop(store);
//
//    }

//    @Test
//    public void multipleStores(){
//        int NCALLS = 1000;
//        Random rand = new Random(System.nanoTime());
//
//        StoreManager manager = new StoreManager();
//        Store<Integer, Integer> store1 = manager.newStore();
//        Store<Integer, Integer> store2 = manager.newStore();
//        Store<Integer, Integer> store3 = manager.newStore();
//        for (int i=0; i<NCALLS; i++) {
//	        System.out.println(i);
//	        int k = rand.nextInt();
//	        int v = rand.nextInt();
//	        store1.put(k, v);
//	        assert rand.nextBoolean() ? store2.get(k).equals(v) : store3.get(k).equals(v);
//	    }
//        manager.stop(store3);
//        manager.stop(store2);
//        manager.stop(store1);
//    }
    
    @Test
    public void dataMigration() {
    	int NCALLS = 10;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        
        List<Store<Integer, Integer>> store = new LinkedList<>();
        store.add(manager.newStore());
        store.add(manager.newStore());
        store.add(manager.newStore());

        for (int i=0; i<NCALLS; i++) {
        	if(i%3 == 0) {
        		store.add(manager.newStore());
        	}
//        	else if(i%55 == 0) {
//        		//stop the last store in the list;
//        		manager.stop(store.get(store.size()-1));
//        		store.remove(store.size()-1);
//        	}
        	int k = rand.nextInt();
	        int v = rand.nextInt();
	        store.get(0).put(k, v);
	        assert rand.nextBoolean() ? store.get(1).get(k).equals(v) : store.get(2).get(k).equals(v);
	    }
        store.stream().forEach(e -> manager.stop(e));
    }
    
//    @Test
//    public void strategyComparison() {
//    	
//    }

}
