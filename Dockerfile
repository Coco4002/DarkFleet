# syntax=docker/dockerfile:1
# Builds one Spring Boot module of the DarkFleet reactor, e.g.
#   docker build --build-arg MODULE=adapter-aisstream -t darkfleet/adapter-aisstream .

FROM maven:3.9.11-eclipse-temurin-25 AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    test -n "$MODULE" && \
    mvn -B -q -pl "$MODULE" -am package -DskipTests

FROM eclipse-temurin:25-jre
ARG MODULE
RUN useradd --system --uid 10001 darkfleet
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-0.1.0-SNAPSHOT.jar app.jar
USER darkfleet
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
