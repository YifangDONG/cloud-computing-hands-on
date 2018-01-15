package org.example.kvstore;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class StoreTest {
/*
    @Test
    public void baseOperations() {
        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store = manager.newStore();
        assert store.get(1) == null;

        store.put(42, 1);
        assert store.get(42).equals(1);
        assert store.put(42, 2).equals(1);
        manager.stop(store);

    }

    @Test
    public void multipleStores(){
        int NCALLS = 1000;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store1 = manager.newStore();
        Store<Integer, Integer> store2 = manager.newStore();
        Store<Integer, Integer> store3 = manager.newStore();
        for (int i=0; i<NCALLS; i++) {
	        int k = rand.nextInt();
	        int v = rand.nextInt();
	        store1.put(k, v);
	        assert rand.nextBoolean() ? store2.get(k).equals(v) : store3.get(k).equals(v);
	    }
        manager.stop(store3);
        manager.stop(store2);
        manager.stop(store1);
    }
   
    @Test
    public void dataMigration() {
    	int NCALLS = 1000;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        
        List<Store<Integer, Integer>> store = new LinkedList<>();
        store.add(manager.newStore());
        store.add(manager.newStore());
        store.add(manager.newStore());

        for (int i=0; i<NCALLS; i++) {
        	if(i%54 == 0) {
        		store.add(manager.newStore());
        	}
        	else if(i%55 == 0) {
        		//stop the last store in the list;
        		manager.stop(store.get(store.size()-1));
        		store.remove(store.size()-1);
        	}
        	int k = rand.nextInt();
	        int v = rand.nextInt();
	        store.get(0).put(k, v);
	        assert rand.nextBoolean() ? store.get(1).get(k).equals(v) : store.get(2).get(k).equals(v);
	    }
        store.stream().forEach(e -> manager.stop(e));
    }
        
    @Test
    public void dataMigrationSimple() {
        Random rand = new Random(System.nanoTime());
        StoreManager manager = new StoreManager();
        int k = rand.nextInt();
        int v = rand.nextInt();
        Store<Integer, Integer> store1 = manager.newStore();
        store1.put(k, v);
        assert store1.get(k).equals(v);
        Store<Integer, Integer> store2 = manager.newStore();
        assert store2.get(k).equals(v);
        manager.stop(store1);
        assert store2.get(k).equals(v);
        manager.stop(store2);
    }
  */ 
   
	@Test
    public void roundRobin() {
    	int NCALLS = 100;
    	final String name = "__dlmKVS";
    	final String mode = "RoundRobin";
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        
        List<Store<Integer, Integer>> store = new LinkedList<>();
        store.add(manager.newStore(name,mode));
        store.add(manager.newStore(name,mode));
        store.add(manager.newStore(name,mode));

        for (int i=1; i<NCALLS; i++) {
            System.out.println(i);

        	if(i%54 == 0) {
        		store.add(manager.newStore(name,mode));
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
