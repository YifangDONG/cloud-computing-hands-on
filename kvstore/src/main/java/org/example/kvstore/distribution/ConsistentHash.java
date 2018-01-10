package org.example.kvstore.distribution;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class ConsistentHash implements Strategy {

	private TreeSet<Integer> ring;
	private Map<Integer, Address> addresses;

	public ConsistentHash(View view) {
		ring = new TreeSet<>();
		addresses = new HashMap<>();
		List<Address> members = view.getMembers();
		for (Address node : members) {
			addresses.put(node.hashCode(), node);
			ring.add(node.hashCode());
		}
	}

	@Override
	public Address lookup(Object key) {
		if (ring.ceiling(key.hashCode()) == null) {
			return addresses.get(ring.first());
		} else {
			return addresses.get(ring.ceiling(key.hashCode()));
		}
	}

}
