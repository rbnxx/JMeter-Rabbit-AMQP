package com.zeroclue.jmeter.protocol.amqp;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.security.*;

import com.rabbitmq.client.*;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;

import com.zeroclue.jmeter.protocol.amqp.ChannelCache;

import org.apache.commons.lang3.StringUtils;

public abstract class AMQPSampler extends AbstractSampler implements ThreadListener {

    public static final boolean DEFAULT_EXCHANGE_DURABLE = true;
    public static final boolean DEFAULT_EXCHANGE_REDECLARE = false;
    public static final boolean DEFAULT_QUEUE_REDECLARE = false;

    public static final int DEFAULT_PORT = 5672;
    public static final String DEFAULT_PORT_STRING = Integer.toString(DEFAULT_PORT);

    public static final int DEFAULT_TIMEOUT = 1000;
    public static final String DEFAULT_TIMEOUT_STRING = Integer.toString(DEFAULT_TIMEOUT);

    public static final int DEFAULT_ITERATIONS = 1;
    public static final String DEFAULT_ITERATIONS_STRING = Integer.toString(DEFAULT_ITERATIONS);
    
    public static final int DEFAULT_HEARTBEAT = 0;
    public static final String DEFAULT_HEARTBEAT_STRING = Integer.toString(DEFAULT_HEARTBEAT);

    private static final Logger log = LoggingManager.getLoggerForClass();


    //++ These are JMX names, and must not be changed
    protected static final String EXCHANGE = "AMQPSampler.Exchange";
    protected static final String EXCHANGE_TYPE = "AMQPSampler.ExchangeType";
    protected static final String EXCHANGE_DURABLE = "AMQPSampler.ExchangeDurable";
    protected static final String EXCHANGE_REDECLARE = "AMQPSampler.ExchangeRedeclare";
    protected static final String QUEUE = "AMQPSampler.Queue";
    protected static final String ROUTING_KEY = "AMQPSampler.RoutingKey";
    protected static final String VIRUTAL_HOST = "AMQPSampler.VirtualHost";
    protected static final String HOST = "AMQPSampler.Host";
    protected static final String PORT = "AMQPSampler.Port";
    protected static final String SSL = "AMQPSampler.SSL";
    protected static final String USERNAME = "AMQPSampler.Username";
    protected static final String PASSWORD = "AMQPSampler.Password";
    private static final String TIMEOUT = "AMQPSampler.Timeout";
    private static final String HEARTBEAT = "AMQPSampler.Heartbeat";
    private static final String SHARED_CONNECTION = "AMQPSampler.SharedConnection";
    private static final String ITERATIONS = "AMQPSampler.Iterations";
    private static final String MESSAGE_TTL = "AMQPSampler.MessageTTL";
    private static final String MESSAGE_EXPIRES = "AMQPSampler.MessageExpires";
    private static final String QUEUE_MAXLENGTH = "AMQPSampler.QueueMaxLength";
    private static final String QUEUE_DURABLE = "AMQPSampler.QueueDurable";
    private static final String QUEUE_REDECLARE = "AMQPSampler.Redeclare";
    private static final String QUEUE_EXCLUSIVE = "AMQPSampler.QueueExclusive";
    private static final String QUEUE_AUTO_DELETE = "AMQPSampler.QueueAutoDelete";
    private static final String POOL_SIZE = "AMQPSampler.PoolSize";
    private static final String SHARED_CONSUMER = "AMQPSampler.SharedConsumer";


    private transient ConnectionFactory factory;
    private transient Connection connection;
    protected transient Channel channel = null;
    protected transient static final ChannelCache channelCache = new ChannelCache();
    protected static ExecutorService consumerPool;

    protected AMQPSampler(){
        factory = new ConnectionFactory();
        log.warn("amqpsampler constructor called");
    }

    protected boolean initChannel() throws IOException, NoSuchAlgorithmException, KeyManagementException {
    	channel = this.getChannel();

        if(channel != null && !channel.isOpen()){
            log.warn("channel " + channel.getChannelNumber()
                    + " closed unexpectedly: ", channel.getCloseReason());
            channel = null; // so we re-open it below
            // remove shared connection from cache
            if(getSharedConnection() != null) {
        		channelCache.set(getCnxKey(), null);
        		// TODO: check that consumer cleaning occur in AMQPConsumer
        		// channelCache.setConsumer(getC, null);
            }
        }

        if(channel == null) {
        	log.info("channel not found, creating it");
        	channel = createChannel();
        	log.info("caching newly created channel");
	       	setChannel(channel);
	    	

            //TODO: Break out queue binding
            boolean queueConfigured = (getQueue() != null && !getQueue().isEmpty());

            if(queueConfigured) {
                if (getQueueRedeclare()) {
                    deleteQueue();
                }

                AMQP.Queue.DeclareOk declareQueueResp = channel.queueDeclare(getQueue(), queueDurable(), queueExclusive(), queueAutoDelete(), getQueueArguments());
            }

            if(!StringUtils.isBlank(getExchange())) { //Use a named exchange
                if (getExchangeRedeclare()) {
                    deleteExchange();
                }

                AMQP.Exchange.DeclareOk declareExchangeResp = channel.exchangeDeclare(getExchange(), getExchangeType(), getExchangeDurable());
                if (queueConfigured) {
                    channel.queueBind(getQueue(), getExchange(), getRoutingKey());
                }
            }

        log.info("bound to:"
                +"\n\t queue: " + getQueue()
                +"\n\t exchange: " + getExchange()
                +"\n\t exchange(D)? " + getExchangeDurable()
                +"\n\t routing key: " + getRoutingKey()
                +"\n\t arguments: " + getQueueArguments()
                );
        }
        return true;
    }

	private String getCnxKey() {
		return ChannelCache.genKey(getVirtualHost(), getHost(), getPort(), getUsername(), getPassword(), getTimeout(), connectionSSL(), getSharedConnection());
	}

    private Map<String, Object> getQueueArguments() {
        Map<String, Object> arguments = new HashMap<String, Object>();

        if(getMessageTTL() != null && !getMessageTTL().isEmpty())
            arguments.put("x-message-ttl", getMessageTTLAsInt());

        if(getMessageExpires() != null && !getMessageExpires().isEmpty())
        	arguments.put("x-expires", getMessageExpiresAsInt());

        if(getQueueMaxLength() != null && !getQueueMaxLength().isEmpty())
    		arguments.put("x-max-length", getQueueMaxLengthAsInt());
        
        return arguments;
    }

    protected Channel getChannel() {
    	
    	// if channel sharing is enabled, look for channel in channelCache
    	if(getSharedConnection() != null) {
    		String cnxkey = getCnxKey();
    		//log.info("looking up channel for key: "+cnxkey+"(get returned: "+channelCache.get(cnxkey)+")");
    		return channelCache.get(cnxkey);
    	} else { // return channel for current AMQPSampler instance
    		return channel;
    	}
    }
    
    protected void setChannel(Channel channel) {
       	if(getSharedConnection() != null) {
       		String cnxkey = getCnxKey();
       		log.info("Caching new channel for connection " + cnxkey);
       		channelCache.set(cnxkey, channel);
       	} else {
       		this.channel = channel;
       	}
    }
    
    protected Connection getConnection() {
    	
    	// if channel sharing is enabled, look for channel in channelCache
    	if(getSharedConnection() != null) {
    		String cnxkey = getCnxKey();
    		log.info("looking up connection for key: "+cnxkey+"(get returned: "+channelCache.getConnection(cnxkey)+")");
    		return channelCache.getConnection(cnxkey);
    	} else { // return channel for current AMQPSampler instance
    		return connection;
    	}
    }
    
    protected void setConnection(Connection connection) {
       	if(getSharedConnection() != null) {
       		String cnxkey = getCnxKey();
       		log.info("Caching new connection for key " + cnxkey);
       		channelCache.setConnection(cnxkey, connection);
       	} else {
       		this.connection = connection;
       	}
    }

    /**
     * @return a string for the sampleResult Title
     */
    protected String getTitle() {
        return this.getName();
    }

    protected int getTimeoutAsInt() {
        if (getPropertyAsInt(TIMEOUT) < 1) {
            return DEFAULT_TIMEOUT;
        }
        return getPropertyAsInt(TIMEOUT);
    }

    public String getTimeout() {
        return getPropertyAsString(TIMEOUT, DEFAULT_TIMEOUT_STRING);
    }


    public void setTimeout(String s) {
        setProperty(TIMEOUT, s);
    }

    public String getIterations() {
        return getPropertyAsString(ITERATIONS, DEFAULT_ITERATIONS_STRING);
    }

    public void setIterations(String s) {
        setProperty(ITERATIONS, s);
    }

    public int getIterationsAsInt() {
        return getPropertyAsInt(ITERATIONS);
    }

    public String getExchange() {
        return getPropertyAsString(EXCHANGE);
    }

    public void setExchange(String name) {
        setProperty(EXCHANGE, name);
    }


    public boolean getExchangeDurable() {
        return getPropertyAsBoolean(EXCHANGE_DURABLE);
    }

    public void setExchangeDurable(boolean durable) {
        setProperty(EXCHANGE_DURABLE, durable);
    }


    public String getExchangeType() {
        return getPropertyAsString(EXCHANGE_TYPE);
    }

    public void setExchangeType(String name) {
        setProperty(EXCHANGE_TYPE, name);
    }


    public Boolean getExchangeRedeclare() {
        return getPropertyAsBoolean(EXCHANGE_REDECLARE);
    }

    public void setExchangeRedeclare(Boolean content) {
        setProperty(EXCHANGE_REDECLARE, content);
    }

    public String getQueue() {
        return getPropertyAsString(QUEUE);
    }

    public void setQueue(String name) {
        setProperty(QUEUE, name);
    }

    public String getSharedConnection() {
        return getPropertyAsString(SHARED_CONNECTION);
    }

    public void setSharedConnection(String name) {
        setProperty(SHARED_CONNECTION, name);
    }

    public String getSharedConsumer() {
        return getPropertyAsString(SHARED_CONSUMER);
    }

    public void setSharedConsumer(String name) {
        setProperty(SHARED_CONSUMER, name);
    }

    public String getRoutingKey() {
        return getPropertyAsString(ROUTING_KEY);
    }

    public void setRoutingKey(String name) {
        setProperty(ROUTING_KEY, name);
    }


    public String getVirtualHost() {
        return getPropertyAsString(VIRUTAL_HOST);
    }

    public void setVirtualHost(String name) {
        setProperty(VIRUTAL_HOST, name);
    }


    public String getMessageTTL() {
        return getPropertyAsString(MESSAGE_TTL);
    }

    public void setMessageTTL(String name) {
        setProperty(MESSAGE_TTL, name);
    }

    protected Integer getMessageTTLAsInt() {
        if (getPropertyAsInt(MESSAGE_TTL) < 1) {
            return null;
        }
        return getPropertyAsInt(MESSAGE_TTL);
    }

    public String getPoolSize() {
        return getPropertyAsString(POOL_SIZE);
    }

    public void setPoolSize(String name) {
        setProperty(POOL_SIZE, name);
    }

    protected Integer getPoolSizeAsInt() {
        return getPropertyAsInt(POOL_SIZE);
    }

    public String getHeartbeat() {
        return getPropertyAsString(HEARTBEAT);
    }

    public void setHeartbeat(String name) {
        setProperty(HEARTBEAT, name);
    }

    protected Integer getHeartbeatAsInt() {
        return getPropertyAsInt(HEARTBEAT);
    }

    public String getMessageExpires() {
        return getPropertyAsString(MESSAGE_EXPIRES);
    }

    public void setMessageExpires(String name) {
        setProperty(MESSAGE_EXPIRES, name);
    }

    protected Integer getMessageExpiresAsInt() {
        if (getPropertyAsInt(MESSAGE_EXPIRES) < 1) {
            return null;
        }
        return getPropertyAsInt(MESSAGE_EXPIRES);
    }


    public String getQueueMaxLength() {
        return getPropertyAsString(QUEUE_MAXLENGTH);
    }

    public void setQueueMaxLength(String length) {
        setProperty(QUEUE_MAXLENGTH, length);
    }

    protected Integer getQueueMaxLengthAsInt() {
        if (getPropertyAsInt(QUEUE_MAXLENGTH) < 1) {
            return null;
        }
        return getPropertyAsInt(QUEUE_MAXLENGTH);
    }

    
    public String getHost() {
        return getPropertyAsString(HOST);
    }

    public void setHost(String name) {
        setProperty(HOST, name);
    }


    public String getPort() {
        return getPropertyAsString(PORT);
    }

    public void setPort(String name) {
        setProperty(PORT, name);
    }

    protected int getPortAsInt() {
        if (getPropertyAsInt(PORT) < 1) {
            return DEFAULT_PORT;
        }
        return getPropertyAsInt(PORT);
    }

    public void setConnectionSSL(String content) {
        setProperty(SSL, content);
    }

    public void setConnectionSSL(Boolean value) {
        setProperty(SSL, value.toString());
    }

    public boolean connectionSSL() {
        return getPropertyAsBoolean(SSL);
    }

    public String getUsername() {
        return getPropertyAsString(USERNAME);
    }

    public void setUsername(String name) {
        setProperty(USERNAME, name);
    }


    public String getPassword() {
        return getPropertyAsString(PASSWORD);
    }

    public void setPassword(String name) {
        setProperty(PASSWORD, name);
    }

    /**
     * @return the whether or not the queue is durable
     */
    public String getQueueDurable() {
        return getPropertyAsString(QUEUE_DURABLE);
    }

    public void setQueueDurable(String content) {
        setProperty(QUEUE_DURABLE, content);
    }

    public void setQueueDurable(Boolean value) {
        setProperty(QUEUE_DURABLE, value.toString());
    }

    public boolean queueDurable(){
        return getPropertyAsBoolean(QUEUE_DURABLE);
    }

    /**
     * @return the whether or not the queue is exclusive
     */
    public String getQueueExclusive() {
        return getPropertyAsString(QUEUE_EXCLUSIVE);
    }

    public void setQueueExclusive(String content) {
        setProperty(QUEUE_EXCLUSIVE, content);
    }

    public void setQueueExclusive(Boolean value) {
        setProperty(QUEUE_EXCLUSIVE, value.toString());
    }

    public boolean queueExclusive(){
        return getPropertyAsBoolean(QUEUE_EXCLUSIVE);
    }

    /**
     * @return the whether or not the queue should auto delete
     */
    public String getQueueAutoDelete() {
        return getPropertyAsString(QUEUE_AUTO_DELETE);
    }

    public void setQueueAutoDelete(String content) {
        setProperty(QUEUE_AUTO_DELETE, content);
    }

    public void setQueueAutoDelete(Boolean value) {
        setProperty(QUEUE_AUTO_DELETE, value.toString());
    }

    public boolean queueAutoDelete(){
        return getPropertyAsBoolean(QUEUE_AUTO_DELETE);
    }


    public Boolean getQueueRedeclare() {
        return getPropertyAsBoolean(QUEUE_REDECLARE);
    }

    public void setQueueRedeclare(Boolean content) {
       setProperty(QUEUE_REDECLARE, content);
    }

    protected void cleanup() {

        try {
            //getChannel().close();   // closing the connection will close the channel if it's still open
        	// When shareChannel is enabled, this will close connection
        	// used by other samplers (their cleanup code won't complete normally)
        	synchronized(getChannel()) {
	       		log.info("closing connection: "+connection);
	            if(connection != null && connection.isOpen())
	                connection.close();
        	}
        } catch (IOException e) {
            log.error("Failed to close connection", e);
        }
    }

    @Override
    public void threadFinished() {
        log.info("AMQPSampler.threadFinished called");
        cleanup();
    }

    @Override
    public void threadStarted() {

    }

    protected Channel createChannel() throws IOException, NoSuchAlgorithmException, KeyManagementException {
        log.info("Creating channel " + getVirtualHost()+":"+getPortAsInt());
        connection = this.getConnection();

        if(getPoolSizeAsInt() != 0 && consumerPool == null)
        	consumerPool = Executors.newFixedThreadPool(getPoolSizeAsInt());

         if (connection == null || !connection.isOpen()) {
        	factory.setRequestedHeartbeat(getHeartbeatAsInt());
            factory.setConnectionTimeout(getTimeoutAsInt());
            factory.setVirtualHost(getVirtualHost());
            factory.setUsername(getUsername());
            factory.setPassword(getPassword());
            if (connectionSSL()) {
                factory.useSslProtocol("TLS");
            }

            log.info("RabbitMQ ConnectionFactory using:"
                  +"\n\t virtual host: " + getVirtualHost()
                  +"\n\t host: " + getHost()
                  +"\n\t port: " + getPort()
                  +"\n\t username: " + getUsername()
                  +"\n\t password: " + getPassword()
                  +"\n\t timeout: " + getTimeout()
                  +"\n\t heartbeat: " + factory.getRequestedHeartbeat()
                  +"\nin " + this
                  );

            String[] hosts = getHost().split(",");
            Address[] addresses = new Address[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                addresses[i] = new Address(hosts[i], getPortAsInt());
            }
            log.info("Using hosts: " + Arrays.toString(hosts) + " addresses: " + Arrays.toString(addresses));
            
            if( consumerPool == null) {
            	connection = factory.newConnection(addresses);
            } else {
            	connection = factory.newConnection(consumerPool, addresses);
            }
         }
         this.setConnection(connection);

         Channel channel = connection.createChannel();
         if(!channel.isOpen()){
             log.fatalError("Failed to open channel: " + channel.getCloseReason().getLocalizedMessage());
         }
        return channel;
    }

    protected void deleteQueue() throws IOException, NoSuchAlgorithmException, KeyManagementException {
        // use a different channel since channel closes on exception.
        Channel channel = createChannel();
        try {
            log.info("Deleting queue " + getQueue());
            channel.queueDelete(getQueue());
        }
        catch(Exception ex) {
            log.debug(ex.toString(), ex);
            // ignore it.
        }
        finally {
            if (channel.isOpen())  {
                channel.close();
            }
        }
    }

    protected void deleteExchange() throws IOException, NoSuchAlgorithmException, KeyManagementException {
        // use a different channel since channel closes on exception.
        Channel channel = createChannel();
        try {
            log.info("Deleting exchange " + getExchange());
            channel.exchangeDelete(getExchange());
        }
        catch(Exception ex) {
            log.debug(ex.toString(), ex);
            // ignore it.
        }
        finally {
            if (channel.isOpen())  {
                channel.close();
            }
        }
    }
}
