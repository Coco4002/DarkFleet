package com.darkfleet.intelligence.web;

import com.darkfleet.intelligence.decisions.Proposals;
import com.darkfleet.security.ApiKeyAuthenticationFilter;
import io.swagger.v3.core.converter.ModelConverters;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springdoc.core.utils.SpringDocUtils;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.JsonNode;

import java.util.stream.Stream;

/**
 * OpenAPI description; Swagger UI's "Authorize" takes the API key. Free-form JSON (evidence) is
 * described as a JSON object, and the three proposal payloads are described in full.
 */
@Configuration(proxyBeanMethods = false)
class OpenApiConfiguration {

    private static final String API_KEY = "apiKey";

    static {
        SpringDocUtils.getConfig().replaceWithSchema(JsonNode.class, new ObjectSchema());
    }

    @Bean
    OpenAPI vesselIntelligenceApi() {
        Components components = new Components();
        Stream.of(Proposals.CasePayload.class, Proposals.WatchlistPayload.class, Proposals.RuleChangePayload.class)
                .map(ModelConverters.getInstance()::readAll)
                .forEach(schemas -> schemas.forEach(components::addSchemas));
        return new OpenAPI()
                .info(new Info()
                        .title("DarkFleet Vessel Intelligence")
                        .version("v1")
                        .description("Vessels, tracks, detections, coverage, sanction lists, cases, watchlist and "
                                + "the proposals an analyst approves. Detections are observations for an analyst "
                                + "to review, never findings of wrongdoing."))
                .components(components.addSecuritySchemes(API_KEY, new SecurityScheme()
                        .type(SecurityScheme.Type.APIKEY)
                        .in(SecurityScheme.In.HEADER)
                        .name(ApiKeyAuthenticationFilter.HEADER)))
                .addSecurityItem(new SecurityRequirement().addList(API_KEY));
    }
}
