FROM eclipse-temurin:25-jre-alpine AS base

WORKDIR /app

ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.9.0/opentelemetry-javaagent.jar otel-agent.jar

COPY build/libs/*.jar app.jar

ENV PORT=8081
ENV DB_URL=""
ENV DB_USERNAME=""
ENV DB_PASSWORD=""
ENV OIDC_ISSUER_URI=""
ENV KAFKA_BOOTSTRAP_SERVERS=""
ENV EVENTS_BROKER=kafka

ENV OTEL_SERVICE_NAME=example-command-service
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
