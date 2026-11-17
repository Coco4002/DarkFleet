package com.darkfleet.intelligence.web;

import com.darkfleet.security.ApiKeyAuthenticationFilter;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** OpenAPI description; Swagger UI's "Authorize" takes the API key. */
@Configuration(proxyBeanMethods = false)
class OpenApiConfiguration {

    private static final String API_KEY = "apiKey";

    @Bean
    OpenAPI vesselIntelligenceApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("DarkFleet Vessel Intelligence")
                        .version("v1")
                        .description("Vessels, tracks, detections, coverage, sanction lists, cases, watchlist and "
                                + "the proposals an analyst approves. Detections are observations for an analyst "
                                + "to review, never findings of wrongdoing."))
                .components(new Components().addSecuritySchemes(API_KEY, new SecurityScheme()
                        .type(SecurityScheme.Type.APIKEY)
                        .in(SecurityScheme.In.HEADER)
                        .name(ApiKeyAuthenticationFilter.HEADER)))
                .addSecurityItem(new SecurityRequirement().addList(API_KEY));
    }
}
