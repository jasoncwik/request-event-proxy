package com.emc.ecs.eventproxy;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

import java.io.IOException;
import java.net.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Created by cwikj on 11/19/15.
 */
public class RequestEventHandler implements HttpHandler {
    public RequestEventHandler(URI amqpUri, String queueName, Set<HttpString> verbFilter, HttpHandler next) throws IOException, TimeoutException,
            KeyManagementException, NoSuchAlgorithmException, URISyntaxException {
        this.next = next;
        this.verbFilter = verbFilter;
        this.queueName = queueName;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(amqpUri);
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, null);
        String message = String.format("START\tProxy starting on host %s", getHostName());
        channel.basicPublish("", queueName, null, message.getBytes());
        System.out.println("RequestEventHandler initialized");
    }

    private HttpHandler next;
    private Channel channel;
    private Set<HttpString> verbFilter;
    private String queueName;

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        // TODO: could log errors here with try/catch
        exchange.addExchangeCompleteListener(new ExchangeCompletionListener() {
            @Override
            public void exchangeEvent(HttpServerExchange exchange, NextListener nextListener) {
                if(exchange.getStatusCode() <= 299) {
                    // Success
                    if(verbFilter.contains(exchange.getRequestMethod())) {
                        String message = String.format("%s\t%d\t%s", exchange.getRequestMethod(), exchange.getStatusCode(), exchange.getRequestURI());
                        try {
                            channel.basicPublish("", queueName, null, message.getBytes());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        next.handleRequest(exchange);

    }

    private String getHostName() {
        // try InetAddress.LocalHost first;
//      NOTE -- InetAddress.getLocalHost().getHostName() will not work in certain environments.
        try {
            String result = InetAddress.getLocalHost().getHostName();
            if (result != null && !result.isEmpty())
                return result;
        } catch (UnknownHostException e) {
            // failed;  try alternate means.
        }

// try environment properties.
//
        String host = System.getenv("COMPUTERNAME");
        if (host != null)
            return host;
        host = System.getenv("HOSTNAME");
        if (host != null)
            return host;

// undetermined.
        return null;
    }
}
