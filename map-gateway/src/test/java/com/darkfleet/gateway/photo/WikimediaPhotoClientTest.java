package com.darkfleet.gateway.photo;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.photo.WikimediaPhotoClient.VesselPhoto;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class WikimediaPhotoClientTest {

    private static final String SAMPLE_JSON = """
            {
              "query": {
                "pages": {
                  "12345": {
                    "pageid": 12345,
                    "ns": 6,
                    "title": "File:Eventin in Baltic.jpg",
                    "imageinfo": [
                      {
                        "thumburl": "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Eventin.jpg/800px-Eventin.jpg",
                        "url": "https://upload.wikimedia.org/wikipedia/commons/a/ab/Eventin.jpg",
                        "descriptionurl": "https://commons.wikimedia.org/wiki/File:Eventin_in_Baltic.jpg",
                        "extmetadata": {
                          "Artist": { "value": "<a href=\\"https://commons.wikimedia.org/wiki/User:Photo\\">Jan Novak</a>" },
                          "LicenseShortName": { "value": "CC BY-SA 4.0" },
                          "LicenseUrl": { "value": "https://creativecommons.org/licenses/by-sa/4.0" }
                        }
                      }
                    ]
                  }
                }
              }
            }
            """;

    @Test
    void parsesImageAndStripsHtmlFromAttribution() {
        VesselPhoto photo = WikimediaPhotoClient.parseResponse(JsonSerde.mapper().readTree(SAMPLE_JSON));
        assertNotNull(photo);
        assertEquals("https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Eventin.jpg/800px-Eventin.jpg",
                photo.imageUrl());
        assertEquals("https://commons.wikimedia.org/wiki/File:Eventin_in_Baltic.jpg", photo.descriptionUrl());
        assertEquals("Jan Novak", photo.author());
        assertEquals("CC BY-SA 4.0", photo.license());
        assertEquals("https://creativecommons.org/licenses/by-sa/4.0", photo.licenseUrl());
        assertEquals("Wikimedia Commons / Jan Novak (CC BY-SA 4.0)", photo.credit());
    }

    @Test
    void returnsNullWhenCategoryHasNoFiles() {
        assertNull(WikimediaPhotoClient.parseResponse(JsonSerde.mapper().readTree("{\"batchcomplete\":\"\"}")));
    }

    @Test
    void followsShipSubcategoryWhenImoCategoryWrapsIt() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/w/api.php", exchange -> {
            int n = requests.incrementAndGet();
            String json = n == 1 ? """
                    {
                      "query": {
                        "pages": {
                          "61679250": {
                            "pageid": 61679250,
                            "ns": 14,
                            "title": "Category:FSRU Exemplar (ship, 2010)"
                          }
                        }
                      }
                    }""" : SAMPLE_JSON;
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/w/api.php";
            GatewayProperties props = new GatewayProperties(false, Duration.ofHours(1), Duration.ofSeconds(5),
                    Duration.ofMinutes(2), "http://localhost:8086", "", Duration.ofSeconds(5),
                    url, Duration.ofSeconds(5), Duration.ofHours(24));
            WikimediaPhotoClient client = new WikimediaPhotoClient(
                    props, Clock.fixed(Instant.parse("2026-11-18T12:00:00Z"), ZoneOffset.UTC));
            VesselPhoto photo = client.lookup("9444649");
            assertNotNull(photo);
            assertEquals("Jan Novak", photo.author());
            assertEquals(2, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void cachesPhotoLookupsAndSkipsInvalidImo() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/w/api.php", exchange -> {
            requests.incrementAndGet();
            byte[] body = SAMPLE_JSON.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/w/api.php";
            GatewayProperties props = new GatewayProperties(false, Duration.ofHours(1), Duration.ofSeconds(5),
                    Duration.ofMinutes(2), "http://localhost:8086", "", Duration.ofSeconds(5),
                    url, Duration.ofSeconds(5), Duration.ofHours(24));
            Clock clock = Clock.fixed(Instant.parse("2026-11-18T12:00:00Z"), ZoneOffset.UTC);
            WikimediaPhotoClient client = new WikimediaPhotoClient(props, clock);

            assertNull(client.lookup(null));
            assertNull(client.lookup("not-an-imo"));
            assertEquals(0, requests.get());

            VesselPhoto first = client.lookup("9308065");
            VesselPhoto second = client.lookup("9308065");
            assertNotNull(first);
            assertEquals(first, second);
            assertEquals(1, requests.get(), "Second lookup for the same IMO must come from the cache");
        } finally {
            server.stop(0);
        }
    }
}
