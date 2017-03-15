package com.emc.ecs.swiftproxy;

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
public class SwiftMeteringProxy {
    private static final String OPT_PROXY_HOST = "proxy-host";
    private static final String OPT_MGMT_URI = "mgmt-uri";
    private static final String OPT_MGMT_USER = "mgmt-user";
    private static final String OPT_MGMT_PASS = "mgmt-pass";
    private static final String OPT_LISTEN_PORT = "listen-port";

    public static void main(String[] args) {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(OPT_PROXY_HOST).desc("URI of the destination ECS or load balancer")
                .hasArg().argName("uri").required().build());
        opts.addOption(Option.builder().longOpt(OPT_MGMT_URI).desc("URI of the ECS Management server, e.g. https://x.x.x.x:4443/")
                .hasArg().argName("uri").required().build());
        opts.addOption(Option.builder().longOpt(OPT_MGMT_USER).desc("ECS Management User (should be system monitor)")
                .hasArg().argName("name").required().build());
        opts.addOption(Option.builder().longOpt(OPT_MGMT_PASS)
                .desc("ECS Management user's password").hasArg().argName("password").build());
        opts.addOption(Option.builder().longOpt(OPT_LISTEN_PORT).desc("Sets the port to listen on, defaults to 9024")
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

        SwiftMeteringProxy proxy = new SwiftMeteringProxy();

        if(cli.hasOption(OPT_PROXY_HOST)) {
            proxy.setProxyHost(cli.getOptionValue(OPT_PROXY_HOST));
        }
        if(cli.hasOption(OPT_MGMT_URI)) {
            proxy.setMgmtUri(cli.getOptionValue(OPT_MGMT_URI));
        }
        if(cli.hasOption(OPT_MGMT_USER)) {
            proxy.setMgmtUser(cli.getOptionValue(OPT_MGMT_USER));
        }
        if(cli.hasOption(OPT_LISTEN_PORT)) {
            proxy.setListenPort(Integer.parseInt(cli.getOptionValue(OPT_LISTEN_PORT)));
        }
        if(cli.hasOption(OPT_MGMT_PASS)) {
            proxy.setMgmtPass(cli.getOptionValue(OPT_MGMT_PASS));
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
    private String mgmtUri;
    private String mgmtUser;
    private String mgmtPass;
    private int listenPort = 9024;

    public void run() throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        HttpHandler proxyHandler = Handlers.proxyHandler(new SimpleProxyClientProvider(new URI(proxyHost)));
        HttpHandler swiftMeteringHandler = new SwiftMeteringHandler(new URI(mgmtUri),
                mgmtUser, mgmtPass, proxyHandler);

        Undertow server = Undertow.builder()
                .addHttpListener(listenPort, "localhost")
                .setHandler(swiftMeteringHandler).build();
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

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }


    public String getMgmtUri() {
        return mgmtUri;
    }

    public void setMgmtUri(String mgmtUri) {
        this.mgmtUri = mgmtUri;
    }

    public String getMgmtUser() {
        return mgmtUser;
    }

    public void setMgmtUser(String mgmtUser) {
        this.mgmtUser = mgmtUser;
    }

    public String getMgmtPass() {
        return mgmtPass;
    }

    public void setMgmtPass(String mgmtPass) {
        this.mgmtPass = mgmtPass;
    }
}
