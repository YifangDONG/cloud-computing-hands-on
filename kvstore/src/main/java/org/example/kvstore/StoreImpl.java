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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StoreImpl<K, V> extends ReceiverAdapter implements Store<K, V> {

	private boolean debuge = false;
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
		if(debuge) {System.out.println(channel.getAddressAsString() +" recive command : " + cmd.toString() + " from node " + dst.toString());}
		workers.submit(new CmdHandler(dst, cmd));
	}

	private class CmdHandler implements Callable<Void> {
		Address address;
		Command<K, V> command;

		public CmdHandler(Address address, Command<K, V> command) {
			this.address = address;
			this.command = command;
		}

		@Override
		public Void call() throws Exception {
			V value = null;
			if (command instanceof Reply) {
				value = command.getValue();
				pending.complete(value);
				return null;
			} else if (command instanceof Get) {
				value = get(command.getKey());
			} else if (command instanceof Put) {
				if(debuge) {System.out.println(command.getKey().toString() + " put " + command.getValue().toString());}
				value = put(command.getKey(), command.getValue());
				if(debuge) {System.out.println(" value " + command.getValue().toString());}

			}
			Reply<K, V> reply = factory.newReplyCmd(command.getKey(), value);
			send(address,reply);
			return null;
		}

	}

	@Override
	public void viewAccepted(View view) {
		this.strategy = new ConsistentHash(view);
	}

	@Override
	public V get(K k) {
		return execute(factory.newGetCmd(k));
	}

	@Override
	public V put(K k, V v) {
		return execute(factory.newPutCmd(k, v));
	}
	
	public V execute(Command cmd) {
		K key = (K) cmd.getKey();
		V value = (V) cmd.getValue();
		Address dst = strategy.lookup(key);
		if(dst.equals(channel.getAddress())) {
			if(debuge) {System.out.println("[local]" + channel.getAddressAsString() +" execute command : " + cmd.toString() + " in node " + dst.toString());}
			if(cmd instanceof Put) {
				V prior = data.get(key);
				data.put(key, value);
				return prior;
			}else if(cmd instanceof Get) {
				return data.get(key);
			}
		} else if (dst != null){
			synchronized(pending) {
				if(debuge) {System.out.println("[remote]" + channel.getAddressAsString() +" execute command : " + cmd.toString() + " in node " + dst.toString());}
				if(cmd instanceof Put) {
					pending = CompletableFuture.supplyAsync(() -> {
						send(dst, factory.newPutCmd(key,value));
						return pending.join();
					});
				}else if(cmd instanceof Get) {
					pending = CompletableFuture.supplyAsync(() -> {
						send(dst, factory.newGetCmd(key));
						return pending.join();

					});
				}
				return pending.join();
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return "Store#" + name + "{" + data.toString() + "}";
	}

}
