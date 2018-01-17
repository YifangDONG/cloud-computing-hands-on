package org.example.abd.quorum;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class Majority {
	private List<Address> quorum;

    public Majority(View view){
    	quorum = new LinkedList<>();
    	while(quorum.size() < view.getMembers().size()/2+1) {
    		Random rand = new Random(System.nanoTime());
    		Address e = view.get(rand.nextInt(view.getMembers().size()));
    		if(!quorum.contains(e))
    		quorum.add(e);
    	}
    }

    public int quorumSize(){
        return quorum.size();
    }

    public List<Address> pickQuorum(){
        return quorum;
    }
}
