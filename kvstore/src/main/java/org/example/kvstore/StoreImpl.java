package org.example.kvstore;

import org.apache.commons.lang.SerializationUtils;
import org.example.kvstore.cmd.Command;
import org.example.kvstore.cmd.CommandFactory;
import org.example.kvstore.cmd.Get;
import org.example.kvstore.cmd.Put;
import org.example.kvstore.cmd.Reply;
import org.example.kvstore.distribution.ConsistentHash;
import org.example.kvstore.distribution.Strategy;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class StoreImpl<K, V> extends ReceiverAdapter implements Store<K, V> {

	private boolean debug = false;
	private boolean debugChangeview = false;

	private String name;
	private Strategy strategy;
	private Map<K, V> data;
	private CommandFactory<K, V> factory;
	private ExecutorService workers;
	private JChannel channel;
	private CompletableFuture<V> pending;

	public StoreImpl(String name) {
		this.name = name;
		this.factory = new CommandFactory<>();
		this.pending = new CompletableFuture<V>();
	}

	public void init() throws Exception {
		this.data = new HashMap<>();
		this.workers = Executors.newCachedThreadPool();
		channel = new JChannel();
		channel.setReceiver(this);
		channel.connect(this.name);

	}
	public void stop() {
		
		this.channel.close();
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
		Command<K,V> cmd = (Command<K,V>) SerializationUtils.deserialize(msg.getBuffer());
		if(debug) {System.out.println(channel.getAddressAsString() +" recive command : " + cmd.toString() + " from node " + dst.toString());}
		workers.submit(() -> {
			V value = null;
			if (cmd instanceof Reply) {
				value = cmd.getValue();
				synchronized(pending) {
					pending.complete(value);
				}
			} else {
				if (cmd instanceof Get) {
					value = data.get(cmd.getKey());
				} else if (cmd instanceof Put) {
					value = data.put(cmd.getKey(), cmd.getValue());
				}
				Reply<K, V> reply = factory.newReplyCmd(cmd.getKey(), value);
				send(dst,reply);
			}
		});
		
	}

	@Override
	public void viewAccepted(View view) {
		if(this.strategy == null) {
			//it is a new node added
			this.strategy = new ConsistentHash(view);
		}else {
			if( view.size() > strategy.size()) {
				//add node in view
				
				//find the new node
				Address newNode = view.getMembers().stream()
						.filter(e -> !strategy.ContainsNode(e))
						.collect(Collectors.toList()).get(0);
				
				//find node next to new node;
				Address changeNode = strategy.lookup(newNode);
				if(debugChangeview) {System.out.println("[new Node]" + newNode.toString());}
				if(debugChangeview) {System.out.println("[change Node]" + changeNode.toString());}

				//change to new view
				this.strategy = new ConsistentHash(view);
				
				if(channel.getAddress() == changeNode) {
					for(Iterator<Entry<K,V>> i = data.entrySet().iterator(); i.hasNext();) {
						Entry<K,V> e = i.next();
						if(strategy.lookup(e.getKey()).equals(channel.getAddress())) {
							//data need to be moved to new node
							put(e.getKey(),e.getValue());
							data.remove(e);
						}
					}
				}
				
			}else {
				//delete node in view
			}
		}
	}

	@Override
	public V get(K k) {
		V result = execute(factory.newGetCmd(k));
		if(debug) {System.out.println(channel.getAddressAsString() + " get result and return :" + result);}
		return result;
	}

	@Override
	public V put(K k, V v) {
		return execute(factory.newPutCmd(k, v));
	}
	
//	public V execute(Command cmd) {
//		K key = (K) cmd.getKey();
//		V value = (V) cmd.getValue();
//		Address dst = strategy.lookup(key);
//		if(dst.equals(channel.getAddress())) {
//			if(debug) {System.out.println("[local]" + channel.getAddressAsString() +" execute command : " + cmd.toString() + " in node " + dst.toString());}
//			if(cmd instanceof Put) {
//				return data.put(key, value);
//			}else if(cmd instanceof Get) {
//				return data.get(key);
//			}
//		} else if (dst != null){
//			synchronized(pending) {
//				if(debug) {System.out.println("[remote]" + channel.getAddressAsString() +" execute command : " + cmd.toString() + " in node " + dst.toString());}
//				if(cmd instanceof Put) {
//					pending = new CompletableFuture<>();
//					send(dst, factory.newPutCmd(key,value));
//					pending.join();
//				}else if(cmd instanceof Get) {
//					pending = CompletableFuture.supplyAsync(() -> {
//						send(dst, factory.newGetCmd(key));
//						return pending.join();
//
//					});
//				}
//				return pending.join();
//			}
//		}
//		return null;
//	}
	
	public V execute(Command cmd) {
		K key = (K) cmd.getKey();
		V value = (V) cmd.getValue();
		Address dst = strategy.lookup(key);
		synchronized(pending) {
			if(debug) {System.out.println(channel.getAddressAsString() +" execute command : " + cmd.toString() + " in node " + dst.toString());}
			if(cmd instanceof Put) {
				pending = new CompletableFuture<>();
				send(dst, factory.newPutCmd(key,value));
			}else if(cmd instanceof Get) {
				pending = new CompletableFuture<>();
				send(dst, factory.newGetCmd(key));
			}
			return pending.join();
		}
	}

	@Override
	public String toString() {
		return "Store#" + name + "{" + data.toString() + "}";
	}

}
