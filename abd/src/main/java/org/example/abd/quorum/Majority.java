package org.example.abd.quorum;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.ArrayList;
import java.util.List;

public class Majority {
	private View view;

    public Majority(View view){
    	this.view = view;
    }

    public int quorumSize(){
        return view.size();
    }

    public List<Address> pickQuorum(){
    	List<Address> quorum = new ArrayList<>();
    	while(quorum.size() < this.quorumSize()/2 + 1) {
    		quorum.add(view.get((int) (Math.random() * this.quorumSize() - 1)));
    	}
        return quorum;
    }

}
