package org.example.kvstore;

import org.apache.commons.lang.SerializationUtils;
import org.example.kvstore.cmd.Command;
import org.example.kvstore.cmd.CommandFactory;
import org.example.kvstore.cmd.Get;
import org.example.kvstore.cmd.Put;
import org.example.kvstore.cmd.Reply;
import org.example.kvstore.distribution.ConsistentHash;
import org.example.kvstore.distribution.RoundRobin;
import org.example.kvstore.distribution.Strategy;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class StoreImpl<K, V> extends ReceiverAdapter implements Store<K, V> {

	private boolean debug = false;
	private boolean debugChangeview = false;

	private final String mode;
	private String name;
	private Strategy strategy;
	private Map<K, V> data;
	private CommandFactory<K, V> factory;
	private ExecutorService workers;
	private JChannel channel;
	private CompletableFuture<V> pending;

	public StoreImpl(String name, String mode) {
		this.name = name;
		this.mode = mode;
		factory = new CommandFactory<>();
		pending = new CompletableFuture<V>();
	}

	public void init() throws Exception {
		data = new ConcurrentHashMap<>();
		workers = Executors.newCachedThreadPool();
		channel = new JChannel();
		channel.setReceiver(this);
		channel.connect(this.name);

	}

	public void stop() {
		if (debugChangeview) {
			System.out.println("[delete Node]" + channel.getAddressAsString());
		}
		if(mode.toUpperCase().equals("CONSISTENTHASH")) {
			strategy.delete(channel.getAddress());
			if (strategy.size() != 0) {
				// there are nodes alive.
				data.entrySet().stream().forEach(e -> {
					put(e.getKey(), e.getValue());
				});
			}
			channel.close();
		} else if(mode.toUpperCase().equals("CONSISTENTHASH")) {
			strategy.delete(channel.getAddress());
			if (strategy.size() != 0) {
				// there are nodes alive.
				data.entrySet().stream().forEach(e -> {
					put(e.getKey(), e.getValue());
				});
			}
			try {
				// wait for all node update finished, really bad solution.
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			channel.close();
		}
	}

	@Override
	public void viewAccepted(View view) {
		if (mode.toUpperCase().equals("CONSISTENTHASH")) {
			if (strategy == null) {
				// it is a new node added
				strategy = new ConsistentHash(view);
			} else {
				if (view.size() > strategy.size()) {
					// add node in view

					// find the new node
					Address newNode = view.getMembers().stream().filter(e -> !strategy.ContainsNode(e))
							.collect(Collectors.toList()).get(0);

					// find node next to new node;
					Address changeNode = strategy.lookup(newNode);
					if (debugChangeview) {
						System.out.println("[new Node]" + newNode.toString());
					}
					if (debugChangeview) {
						System.out.println("[change Node]" + changeNode.toString());
					}

					// change to new view
					strategy = new ConsistentHash(view);

					if (channel.getAddress() == changeNode) {
						// Map<K, V> dataDelete = new HashMap<>();
						for (Iterator<Entry<K, V>> i = data.entrySet().iterator(); i.hasNext();) {
							Entry<K, V> e = i.next();
							if (!strategy.lookup(e.getKey()).equals(channel.getAddress())) {
								// data need to be moved to new node
								put(e.getKey(), e.getValue());
								data.remove(e.getKey(), e.getValue());
							}
						}
					}

				} else {
					// delete node in view
					strategy = new ConsistentHash(view);
				}
			}
		} else if (mode.toUpperCase().equals("ROUNDROBIN")) {
			strategy = new RoundRobin(view);
			if (debug) {
				System.out.println(channel.getAddressAsString() + " replace data");
			}
			for (Iterator<Entry<K, V>> i = data.entrySet().iterator(); i.hasNext();) {
				Entry<K, V> e = i.next();
				if (!strategy.lookup(e.getKey()).equals(channel.getAddress())) {
					put(e.getKey(), e.getValue());
					data.remove(e.getKey(), e.getValue());
				}
			}
			try {
				// wait for all node update finished, really bad solution.
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (debug) {
				System.out.println(channel.getAddressAsString() + " replace data finished");
			}
		}
	}

	public void send(Address dst, Command<K, V> command) {
		Message msg = new Message(dst, null, SerializationUtils.serialize(command));
		try {
			channel.send(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void receive(Message msg) {
		Address dst = msg.getSrc();
		Command<K, V> cmd = (Command<K, V>) SerializationUtils.deserialize(msg.getBuffer());
		if (debug) {
			System.out.println(channel.getAddressAsString() + " recive command : " + cmd.toString() + " from node "
					+ dst.toString());
		}
		workers.submit(() -> {
			V value = null;
			if (cmd instanceof Reply) {
				value = cmd.getValue();
				synchronized (pending) {
					pending.complete(value);
				}
			} else {
				if (cmd instanceof Get) {
					value = data.get(cmd.getKey());
				} else if (cmd instanceof Put) {
					value = data.put(cmd.getKey(), cmd.getValue());
				}
				Reply<K, V> reply = factory.newReplyCmd(cmd.getKey(), value);
				send(dst, reply);
			}
		});

	}

	@Override
	public V get(K k) {
		Address dst = strategy.lookup(k);

		synchronized (pending) {
			if (debug) {
				System.out.println(
						channel.getAddressAsString() + " execute command : Get{" + k + "} in node " + dst.toString());
			}
			pending = new CompletableFuture<>();
			send(dst, factory.newGetCmd(k));
			if (debug) {
				System.out.println(pending.join());
			}
			return pending.join();
		}
	}

	@Override
	public V put(K k, V v) {
		Address dst = strategy.lookup(k);
		synchronized (pending) {
			if (debug) {
				System.out.println(channel.getAddressAsString() + " execute command : Put{" + k + "," + v + "} in node "
						+ dst.toString());
			}
			pending = new CompletableFuture<>();
			send(dst, factory.newPutCmd(k, v));
			return pending.join();
		}
	}

	@Override
	public String toString() {
		return "Store#" + name + "{" + data.toString() + "}";
	}

}
