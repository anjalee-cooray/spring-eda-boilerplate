FROM eclipse-temurin:25-jre-alpine AS base

WORKDIR /app

COPY build/libs/*.jar app.jar

ENV PORT=8888
ENV KAFKA_BOOTSTRAP_SERVERS=""
ENV CONFIG_REPO_PATH="file:/workspace/config-repo"
ENV LOGGING_LOKI_URL=http://loki:3100

EXPOSE ${PORT}

ENTRYPOINT ["java", \
  "--enable-preview", \
  "-jar", "app.jar"]
