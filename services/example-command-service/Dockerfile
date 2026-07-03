FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

COPY build/libs/*.jar app.jar

ENV PORT=8081
ENV DB_URL=""
ENV DB_USERNAME=""
ENV DB_PASSWORD=""
ENV OIDC_ISSUER_URI=""
ENV KAFKA_BOOTSTRAP_SERVERS=""
ENV EVENTS_BROKER=kafka

EXPOSE ${PORT}

ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
