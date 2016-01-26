package com.zeroclue.jmeter.protocol.amqp;

import java.util.HashMap;
import java.util.Map;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;


/**
 * @author rbnxx
 * ChannelCache caches Channel objects for a given thread (user) with the 
 * same connection parameters (ie. if several AMQPSamplers are configured
 * to use the same broker, they will share the same connection and channel). 
 *
 */
class ChannelCache {
	
	private static final Logger log = LoggingManager.getLoggerForClass();

	private final ThreadLocal<Map<String,Connection>> cnxMap = new ThreadLocal<Map<String,Connection>>(){
		{
			log.debug("initializing ChannelCache \"Connection\" Map (global)");
		}

		@Override protected Map<String,Connection> initialValue() {
			log.debug("initializing ChannelCache HashMap for thread");
			return new HashMap<String,Connection>();
		}
		
	};

	
	private final ThreadLocal<Map<String,Channel>> cnxChannelMap = new ThreadLocal<Map<String,Channel>>(){
		{
			log.debug("initializing ChannelCache (global)");
		}

		@Override protected Map<String,Channel> initialValue() {
			log.debug("initializing ChannelCache HashMap for thread");
			return new HashMap<String,Channel>();
		}
		
	};

	private final ThreadLocal<Map<String,QueueingConsumer>> consumerMap = new ThreadLocal<Map<String,QueueingConsumer>>(){
		{
			log.debug("initializing ChannelCache (global)");
		}

		@Override protected Map<String,QueueingConsumer> initialValue() {
			log.debug("initializing ChannelCache HashMap for thread");
			return new HashMap<String,QueueingConsumer>();
		}
		
	};
	
	public static String genKey(String vhost, String host, String port, String user, String pass, String timeout, Boolean ssl, String cnxAlias) {
		// generated as amqp uri (cf. https://www.rabbitmq.com/uri-query-parameters.html )
		return new StringBuilder()
				.append(ssl?"amqps://":"amqp://")
				.append(user)
				.append(":")
				.append(pass)
				.append("@host:")
				.append(host)
				.append(":")
				.append(port)
				.append("/")
				.append(vhost)
				.append("?connection_timeout=")
				.append(timeout)
				.append("&alias=")
				.append(cnxAlias)
				.toString();
	}
	
	public void set(String cnxString, Channel channel) {
		cnxChannelMap.get().put(cnxString, channel);
	}
	
	public Channel get(String cnxString) {
		return cnxChannelMap.get().get(cnxString);
	}
	
	public void setConsumer(String cnxString, QueueingConsumer consumer) {
		consumerMap.get().put(cnxString, consumer);
	}
	
	public QueueingConsumer getConsumer(String cnxString) {
		return consumerMap.get().get(cnxString);
	}
	
	public void setConnection(String cnxString, Connection connection) {
		cnxMap.get().put(cnxString, connection);
	}
	
	public Connection getConnection(String cnxString) {
		return cnxMap.get().get(cnxString);
	}

};

