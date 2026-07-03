FROM eclipse-temurin:25-jre-alpine AS base

WORKDIR /app

# Download OTel Java agent
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.9.0/opentelemetry-javaagent.jar otel-agent.jar

COPY build/libs/*.jar app.jar

ENV PORT=8080
ENV OIDC_ISSUER_URI=""
ENV COMMAND_SERVICE_URL=""
ENV QUERY_SERVICE_URL=""
ENV REDIS_HOST=""
ENV REDIS_PORT=6379

# OTel config — override per environment
ENV OTEL_SERVICE_NAME=api-gateway
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317
ENV OTEL_EXPORTER_OTLP_PROTOCOL=grpc
ENV OTEL_METRICS_EXPORTER=none
ENV OTEL_LOGS_EXPORTER=none

ENV LOGGING_LOKI_URL=http://loki:3100

EXPOSE ${PORT}

ENTRYPOINT ["java", \
  "--enable-preview", \
  "-javaagent:/app/otel-agent.jar", \
  "-jar", "app.jar"]
