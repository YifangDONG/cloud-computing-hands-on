package org.example.kvstore.distribution;

import org.jgroups.Address;

public interface Strategy {
	
    Address lookup(Object key);
    
    boolean ContainsNode(Address node);
    
    int size();
    
    void delete(Address node);
}