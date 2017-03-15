package com.emc.ecs.swiftproxy;

import io.undertow.attribute.RequestMethodAttribute;
import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;

import java.io.IOException;
import java.net.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by cwikj on 11/19/15.
 */
public class SwiftMeteringHandler implements HttpHandler {
    public SwiftMeteringHandler(URI mgmtUri, String mgmtUser, String mgmtPass, HttpHandler next) throws IOException, TimeoutException,
            KeyManagementException, NoSuchAlgorithmException, URISyntaxException {
        this.next = next;
        this.mgmtUri = mgmtUri;
        this.mgmtUser = mgmtUser;
        this.mgmtPass = mgmtPass;

        accountPattern = Pattern.compile(ACCOUNT_URI);
        containerPattern = Pattern.compile(CONTAINER_URI);

        System.out.println("SwiftMeteringHandler initialized");
    }

    private HttpHandler next;
    private URI mgmtUri;
    private String mgmtUser;
    private String mgmtPass;
    private Pattern accountPattern;
    private Pattern containerPattern;

    private static final String ACCOUNT_URI = "^/v1/([^/]+)/?$";
    private static final String CONTAINER_URI = "^/v1/([^/]+)/([^/]+)/?$";
    private static final HttpString ACCOUNT_BYTES = HttpString.tryFromString("X-Account-Bytes-Used");
    private static final HttpString ACCOUNT_OBJS = HttpString.tryFromString("X-Account-Object-Count");
    private static final HttpString CONTAINER_BYTES = HttpString.tryFromString("X-Container-Bytes-Used");
    private static final HttpString CONTAINER_OBJS = HttpString.tryFromString("X-Container-Object-Count");

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        // First, only intercept GET/HEAD /v1/{account} and HEAD/GET(?) /v1/{account}/{container}
        if(exchange.getRequestMethod().equals(Methods.GET) || exchange.getRequestMethod().equals(Methods.HEAD)) {
            // TODO: figure out how to use undertow's handlers to do this automatically
            String path = exchange.getRequestPath();

            Matcher m = accountPattern.matcher(path);
            if(m.matches()) {
                // GET/HEAD account
                final String namespace = m.group(1);

                exchange.addExchangeCompleteListener(new ExchangeCompletionListener() {
                    @Override
                    public void exchangeEvent(HttpServerExchange exchange, NextListener nextListener) {
                        // Only work on successful operations
                        if (exchange.getStatusCode() <= 299) {
                            // TODO: Call management API to get namespace metering.

                            HeaderMap headers = exchange.getResponseHeaders();

                            // Remove bad data
                            headers.remove(ACCOUNT_BYTES);
                            headers.remove(ACCOUNT_OBJS);

                            // TODO: Update headers with metering data

                        }
                    }
                });
            } else {
                m = containerPattern.matcher(path);

                if(m.matches()) {
                    // GET/HEAD container
                    final String namespace = m.group(1);
                    final String bucket = m.group(2);

                    exchange.addExchangeCompleteListener(new ExchangeCompletionListener() {
                        @Override
                        public void exchangeEvent(HttpServerExchange exchange, NextListener nextListener) {
                            // Only work on successful operations
                            if (exchange.getStatusCode() <= 299) {
                                // TODO: Call management API to get namespace metering.  Probably should cache this
                                // for a short time in case there's repeated calls to GET container.

                                HeaderMap headers = exchange.getResponseHeaders();

                                // Remove bad data
                                headers.remove(CONTAINER_BYTES);
                                headers.remove(CONTAINER_OBJS);

                                // TODO: Update headers with metering data

                            }
                        }
                    });

                }
            }
        }


        next.handleRequest(exchange);

    }

}
