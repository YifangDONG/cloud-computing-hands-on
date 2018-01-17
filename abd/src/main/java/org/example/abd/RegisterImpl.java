package org.example.abd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.SerializationUtils;
import org.example.abd.cmd.Command;
import org.example.abd.cmd.CommandFactory;
import org.example.abd.cmd.ReadReply;
import org.example.abd.cmd.ReadRequest;
import org.example.abd.cmd.WriteReply;
import org.example.abd.cmd.WriteRequest;
import org.example.abd.quorum.Majority;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class RegisterImpl<V> extends ReceiverAdapter implements Register<V> {

	private boolean debug = true;
	private String name;
	private boolean isWritable;
	private V value;
	private int label;
	private int max;
	private Majority quorumSystem;
	private CompletableFuture<Command> pending;

	private CommandFactory<V> factory;
	private JChannel channel;

	public RegisterImpl(String name) {
		this.name = name;
		this.factory = new CommandFactory<>();
	}

	public void init(boolean isWritable) throws Exception {
		value = null;
		label = 0;
		max = 0;		
		if(debug) {System.out.println(isWritable);}
		this.isWritable = isWritable;
		
		channel = new JChannel();
		if(debug) {System.out.println("channel create success");}
		channel.setReceiver(this);
		if(debug) {System.out.println("channel setReceiver success");}
		if(debug) {System.out.println(this.name);}
		channel.connect(this.name);
		if(debug) {System.out.println("channel connect success");}
		
	}

	@Override
	public void viewAccepted(View view) {
		quorumSystem = new Majority(view);
	}

	// Client part

	@Override
	public V read() {
		List<Command> replys = new ArrayList<>();
		for (Address e : quorumSystem.pickQuorum()) {
			pending = new CompletableFuture<>();
			send(e, factory.newReadRequest());
			replys.add(pending.join());
		}
		if(debug) {System.out.println(replys);}
		int maxTag = replys.get(0).getTag();
		V value = (V) replys.get(0).getValue();
		for (Command<V> c : replys) {
			if (c.getTag() > maxTag) {
				maxTag = c.getTag();
				value = c.getValue();
			}
		}
		return value;
	}

	@Override
	public void write(V v) {
		if(isWritable) {
			this.label = ++this.max;
			for (Address e : this.quorumSystem.pickQuorum()) {
				pending = new CompletableFuture<>();
				send(e, factory.newWriteRequest(value, this.label));
				pending.join();
			}
		}else {
			throw new IllegalStateException();
		}
	}
	

	// Message handlers

	@Override
	public void receive(Message msg) {
		Address sender = msg.getSrc();
		Command<V> cmd = SerializationUtils.deserialize(msg.getBuffer());
		if (cmd instanceof ReadRequest) {
			send(sender, factory.newReadReply(this.value, this.label));
		} else if (cmd instanceof WriteRequest) {
			if (cmd.getTag() > this.label) {
				this.value = cmd.getValue();
				this.label = cmd.getTag();
			}
			send(sender, factory.newWriteReply());
		} else if (cmd instanceof ReadReply || cmd instanceof WriteReply) {
			pending.complete(cmd);
		}
	}

	private void send(Address dst, Command command) {
		try {
			Message message = new Message(dst, channel.getAddress(), SerializationUtils.serialize(command));
			channel.send(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
