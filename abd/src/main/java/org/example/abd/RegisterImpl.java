package org.example.abd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

public class RegisterImpl<V> extends ReceiverAdapter implements Register<V>{

    private String name;
    private boolean isWritable;
    private V value;
    private int label;
    private int max;
    private Majority quorumSystem;
    private CompletableFuture<Command<V>> pending;
    
    private CommandFactory<V> factory;
    private JChannel channel;

    public RegisterImpl(String name) {
        this.name = name;
        this.factory = new CommandFactory<>();
    }

    public void init(boolean isWritable) throws Exception{
    	 this.value = null;
         this.label = 0;
         this.max = 0;
         this.isWritable = isWritable;
         channel = new JChannel();
         channel.setReceiver(this);
         channel.connect(this.name);
         this.quorumSystem = new Majority(this.channel.getView());
    }

    @Override
    public void viewAccepted(View view) {}

    // Client part

    @Override
    public V read() {
    	List<Command<V>> rst = new ArrayList<>();
    	for(Address e : this.channel.getView().getMembers()) {
    		pending = CompletableFuture.supplyAsync(() -> {
    			send(e, factory.newReadRequest());
    			return pending.join();
    		});
    	}
		for(int i = 0; i < this.channel.getView().getMembers().size(); i++) {
    		rst.add(pending.join());
		}
		int maxTag = rst.get(0).getTag();
		V value = rst.get(0).getValue();
		for(Command<V> c : rst) {
			if(c.getTag() > maxTag) {
				maxTag = c.getTag();
				value = c.getValue();
			}
		}
        return value;
    }

    @Override
    public void write(V v) {
    	this.label = ++this.max;
    	for(Address e : this.quorumSystem.pickQuorum()) {
    		pending = CompletableFuture.supplyAsync(() -> {
    			send(e,factory.newWriteRequest(value, this.label));
    			return pending.join();
    		});
    	}
		for(int i = 0; i < this.quorumSystem.quorumSize(); i++) {
			pending.join();
		}
    }

    private V execute(Command cmd){
    	if(cmd instanceof ReadRequest) {
    		return this.read();
    	} else if (cmd instanceof WriteRequest) {
    		if(this.isWritable == false) {
    			new IllegalStateException();
    		} else {
    			this.write((V) cmd.getValue());
    			return null;
    		}	 
    	}

        return null;
    }

    // Message handlers
    private class CmdHandler implements Callable<Void> {

    	Address addr;
    	Command cmd;
    	
    	CmdHandler(Address addr, Command cmd) {
    		this.addr = addr;
    		this.cmd = cmd;
    	}
		
		@Override
		public Void call() throws Exception {
			if(cmd instanceof ReadReply) {
				return null;
			}else if(cmd instanceof WriteReply) {
				return null;
			}else if(cmd instanceof ReadRequest || cmd instanceof WriteRequest) {
				execute(cmd);
				return null;
			} 
			return null;
		}
    	
    }

    @Override
    public void receive(Message msg) {
    	Address sender = msg.getSrc();
    	byte[] content = msg.getBuffer();
    	Command<V> cmd = SerializationUtils.deserialize(content);
    	if(cmd instanceof ReadRequest) {
    		send(sender, factory.newReadReply(this.value, this.label));
    	} else if(cmd instanceof WriteRequest) {
    		if(cmd.getTag() > this.label) {
    			this.value = cmd.getValue();
    			this.label = cmd.getTag();
    		}
    		send(sender,factory.newWriteReply());
    	} else if(cmd instanceof ReadReply || cmd instanceof WriteReply) {
    		pending.complete(cmd);
    	}
    }

    private void send(Address dst, Command command) {
        try {
            Message message = new Message(dst,channel.getAddress(), SerializationUtils.serialize(command));
            channel.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
