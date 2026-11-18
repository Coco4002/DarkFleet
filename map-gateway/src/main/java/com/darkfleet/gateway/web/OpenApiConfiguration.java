package com.darkfleet.gateway.web;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.JsonSchema;
import org.springdoc.core.utils.SpringDocUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.JsonNode;

import java.util.LinkedHashSet;
import java.util.List;

@Configuration(proxyBeanMethods = false)
class OpenApiConfiguration {

    static {
        SpringDocUtils.getConfig().replaceWithSchema(JsonNode.class, new JsonSchema()
                .types(new LinkedHashSet<>(List.of("object", "array", "string", "number", "boolean", "null")))
                .description("Any JSON value"));
    }

    @Bean
    OpenAPI mapGatewayApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("DarkFleet Map Gateway")
                        .version("v1")
                        .description("Public read-only API for the DarkFleet live map: live vessel snapshot stream "
                                + "(SSE), public ship panel with flag and Wikimedia Commons photo, 7-day time-slider "
                                + "history and H3 coverage cells. Never exposes detections, cases, watchlist or "
                                + "analyst data."));
    }
}
