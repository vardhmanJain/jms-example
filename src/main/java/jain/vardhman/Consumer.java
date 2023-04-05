
package jain.vardhman;

// ActiveMQ JMS Provider
import org.apache.qpid.jms.JmsConnectionFactory;


// JMS API types
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import java.util.Enumeration;

import javax.jms.*;

class Consumer {

    public static void main(String[] args) throws Exception {

    	/*  Every JMS provider (every library that implements the JMS API) 
    	 *  will have its own implementation of the javax.jms.ConnectionFactory. 
    	 * 
    	 *  The purpose of the ConnectionFactory is to create a network connection 
    	 *  to a specific JMS broker, such as ActiveMQ, or a specific protocol,
    	 *  such as AMQP.  This allows the JMS library to send and receive messages
    	 *  over a network from the broker.
    	 * 
    	 *  In this case we are using the Apache Qpid JMS library which is specific 
    	 *  to the protocol, AMQP. AMQP is only one of ten protocols currently supported by
    	 *  ActiveMQ.
    	 */
        JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
        Connection connection = factory.createConnection("admin", "password");
        connection.start();
        
        /*  Every JMS Connection can have multiple sessions which manage things like
         *  transactions and message persistence separately.  In practice multiple sessions
         *  are not used much by developers but may be used in more sophisticated
         *  application servers to conserve resources.
         */
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        /*  A Destination is an address of a specific Topic or Queue hosted by the 
         *  JMS broker. The only difference between using JMS for a Topic (Pub/Sub) 
         *  and Queue (P2P) is this bit of code here - at least in the simplest 
         *  cases.  
         *  
         *  That said, there are significant differences between Topic- and 
         *  Queue-based messaging and understanding those differences is key to 
         *  understanding JMS and messaging systems in general
         */
        Destination destination = null;             	
        destination = session.createTopic("MyTopic");
        
        /*  A MessageConsumer is specific to a destination - it can only
         *  receive messages from a specific Topic or Queue.
		 */
        String selector = "STREAM = '2.13'";
        MessageConsumer consumer = session.createConsumer(destination,selector);
        

        String body;
        do {
        	
        	/* To consume messages from Topics or Queues you call the receive() method
        	 * on the MessageConsumer. That method will pause the application (block the 
        	 * thread) until a message is delivered from the Topic or Queue.
        	 * 
        	 * As soon as the API detects that a new message has reached a Topic or Queue, 
        	 * it will alert the JMS consumers and deliver the message. 
        	 * 
        	 * In the case of a Topic, every JMS client listening to the same
        	 * Topic gets a copy of the message. In the case of a Queue, the JMS clients 
        	 * wait in a queue (thus the name). When a message is delivered to the Queue
        	 * the JMS client in the front gets the message and moves to the back of the 
        	 * queue so that the next JMS client in line gets the next message.
        	 */
            Message msg = consumer.receive();
            body = ((TextMessage) msg).getText();
            System.out.println("Received = "+body);
            
        }while (!body.equalsIgnoreCase("SHUTDOWN"));
        
        /* As is the case with most enterprise resources, you want to shut a JMS connection
         * down when you are done using it.  This tells the JMS broker that it can free
         * up the network resources used for that connection making the whole system more 
         * scalable.
         */
        connection.close();
        System.exit(1);
    }
}