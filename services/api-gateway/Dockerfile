FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

COPY build/libs/*.jar app.jar

ENV PORT=8080
ENV OIDC_ISSUER_URI=""
ENV COMMAND_SERVICE_URL=""
ENV QUERY_SERVICE_URL=""
ENV REDIS_HOST=""
ENV REDIS_PORT=6379

EXPOSE ${PORT}

ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
