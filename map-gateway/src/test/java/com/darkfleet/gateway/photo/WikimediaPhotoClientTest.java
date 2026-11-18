package com.darkfleet.gateway.photo;

import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.gateway.GatewayProperties;
import com.darkfleet.gateway.TestProperties;
import com.darkfleet.gateway.photo.WikimediaPhotoClient.VesselPhoto;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
            GatewayProperties props = TestProperties.gateway(false, url);
            WikimediaPhotoClient client = new WikimediaPhotoClient(props, Clock.systemUTC());
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
            GatewayProperties props = TestProperties.gateway(false, url);
            WikimediaPhotoClient client = new WikimediaPhotoClient(props, Clock.systemUTC());

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

    @Test
    void aFailedLookupIsTriedAgainNextTime() throws Exception {
        assertFailureNotCached(503, "{}");
        // the API's own errors, throttling included, come with HTTP 200
        assertFailureNotCached(200, "{\"error\":{\"code\":\"ratelimited\",\"info\":\"slow down\"}}");
    }

    private static void assertFailureNotCached(int failStatus, String failBody) throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/w/api.php", exchange -> {
            boolean fail = requests.incrementAndGet() == 1;
            byte[] body = (fail ? failBody : SAMPLE_JSON).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(fail ? failStatus : 200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/w/api.php";
            WikimediaPhotoClient client = new WikimediaPhotoClient(TestProperties.gateway(false, url), Clock.systemUTC());

            assertNull(client.lookup("9308065"));
            assertNotNull(client.lookup("9308065"));
            assertEquals(2, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void sendsNothingWhileWikimediaAsksToWait() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/w/api.php", exchange -> {
            requests.incrementAndGet();
            exchange.getResponseHeaders().set("Retry-After", "60");
            exchange.sendResponseHeaders(429, -1);
            exchange.close();
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/w/api.php";
            MutableClock clock = new MutableClock(Instant.parse("2026-11-18T12:00:00Z"));
            WikimediaPhotoClient client = new WikimediaPhotoClient(TestProperties.gateway(false, url), clock);

            assertNull(client.lookup("9308065"));
            assertNull(client.lookup("9444649"));
            assertEquals(1, requests.get(), "no request while Retry-After is in force");

            clock.now = clock.now.plusSeconds(61);
            assertNull(client.lookup("9444649"));
            assertEquals(2, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aLookupWaitingWhileWikimediaThrottlesSendsNothing() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        CountDownLatch firstArrived = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/w/api.php", exchange -> {
            requests.incrementAndGet();
            firstArrived.countDown();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.getResponseHeaders().set("Retry-After", "60");
            exchange.sendResponseHeaders(429, -1);
            exchange.close();
        });
        server.start();
        try (ExecutorService lookups = Executors.newVirtualThreadPerTaskExecutor()) {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/w/api.php";
            WikimediaPhotoClient client = new WikimediaPhotoClient(TestProperties.gateway(false, url), Clock.systemUTC());

            Future<VesselPhoto> first = lookups.submit(() -> client.lookup("9308065"));
            firstArrived.await();
            Future<VesselPhoto> second = lookups.submit(() -> client.lookup("9444649"));

            assertNull(first.get());
            assertNull(second.get());
            assertEquals(1, requests.get(), "the waiting lookup must see the Retry-After the first one got");
        } finally {
            server.stop(0);
        }
    }

    private static final class MutableClock extends Clock {
        Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
