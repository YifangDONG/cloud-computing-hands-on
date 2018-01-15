package org.example.kvstore.distribution;

import java.util.LinkedList;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.View;

public class RoundRobin implements Strategy {

	private List<Address> addresses;

	public RoundRobin(View view) {
		addresses = new LinkedList<>();
		List<Address> members = view.getMembers();
		members.stream().forEach(e -> {
			addresses.add(e);
		});
	}

	@Override
	public Address lookup(Object key) {
		return addresses.get(Math.abs(key.hashCode()) % addresses.size());
	}

	@Override
	public boolean ContainsNode(Address node) {
		return (addresses.contains(node));
	}

	@Override
	public int size() {
		return addresses.size();
	}

	@Override
	public void delete(Address node) {
		addresses.remove(node);
	}
}
