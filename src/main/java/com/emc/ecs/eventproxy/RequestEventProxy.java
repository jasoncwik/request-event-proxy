package com.emc.ecs.eventproxy;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.proxy.SimpleProxyClientProvider;
import io.undertow.util.HttpString;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Created by cwikj on 11/19/15.
 */
public class RequestEventProxy {
    private static final String OPT_PROXY_HOST = "proxy-host";
    private static final String OPT_AMQP_URI = "amqp-uri";
    private static final String OPT_QUEUE_NAME = "queue-name";
    private static final String OPT_VERB_FILTER = "verb-filter";
    private static final String OPT_LISTEN_PORT = "listen-port";

    public static void main(String[] args) {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(OPT_PROXY_HOST).desc("URI of the destination ECS or load balancer")
                .hasArg().argName("uri").required().build());
        opts.addOption(Option.builder().longOpt(OPT_AMQP_URI).desc("URI of the AMQP server")
                .hasArg().argName("uri").required().build());
        opts.addOption(Option.builder().longOpt(OPT_QUEUE_NAME).desc("Name of queue on server to publish events to")
                .hasArg().argName("name").required().build());
        opts.addOption(Option.builder().longOpt(OPT_VERB_FILTER)
                .desc("Comma-separated list of verbs to notify on, defaults to PUT")
                .hasArgs().argName("verb,verb,verb...").build());
        opts.addOption(Option.builder().longOpt(OPT_LISTEN_PORT).desc("Sets the port to listen on, defaults to 9020")
                .hasArg().argName("port").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = parser.parse(opts, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(opts);
            System.exit(255);
        }

        RequestEventProxy proxy = new RequestEventProxy();

        if(cli.hasOption(OPT_PROXY_HOST)) {
            proxy.setProxyHost(cli.getOptionValue(OPT_PROXY_HOST));
        }
        if(cli.hasOption(OPT_AMQP_URI)) {
            proxy.setAmqpUri(cli.getOptionValue(OPT_AMQP_URI));
        }
        if(cli.hasOption(OPT_QUEUE_NAME)) {
            proxy.setQueueName(cli.getOptionValue(OPT_QUEUE_NAME));
        }
        if(cli.hasOption(OPT_LISTEN_PORT)) {
            proxy.setListenPort(Integer.parseInt(cli.getOptionValue(OPT_LISTEN_PORT)));
        }
        if(cli.hasOption(OPT_VERB_FILTER)) {
            proxy.setVerbFilter(cli.getOptionValues(OPT_VERB_FILTER));
        }

        try {
            proxy.run();
        } catch (Exception e) {
            System.err.println("Error running proxy server: " + e);
            e.printStackTrace();
            System.exit(1);
        }

    }

    private static void printHelp(Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar request-event-proxy-{version}.jar", opts);
    }

    private String proxyHost;
    private String amqpUri;
    private String queueName;
    private String[] verbFilter = new String[] { "PUT" };
    private int listenPort = 9020;

    public void run() throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        HttpHandler proxyHandler = Handlers.proxyHandler(new SimpleProxyClientProvider(new URI(proxyHost)));
        HttpHandler requestEvehtnHandler = new RequestEventHandler(new URI(amqpUri),
                queueName, buildVerbSet(verbFilter), proxyHandler);

        Undertow server = Undertow.builder()
                .addHttpListener(listenPort, "localhost")
                .setHandler(requestEvehtnHandler).build();
        server.start();
    }

    private Set<HttpString> buildVerbSet(String[] verbFilter) {
        HashSet<HttpString> verbs = new HashSet<>();
        for(String v : verbFilter) {
            verbs.add(new HttpString(v));
        }

        return verbs;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public String getAmqpUri() {
        return amqpUri;
    }

    public void setAmqpUri(String amqpUri) {
        this.amqpUri = amqpUri;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String[] getVerbFilter() {
        return verbFilter;
    }

    public void setVerbFilter(String[] verbFilter) {
        this.verbFilter = verbFilter;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }
}
