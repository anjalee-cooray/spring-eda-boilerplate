FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

COPY build/libs/*.jar app.jar

ENV PORT=8084
ENV DB_URL=""
ENV DB_USERNAME=""
ENV DB_PASSWORD=""
ENV KAFKA_BOOTSTRAP_SERVERS=""
ENV EVENTS_BROKER=kafka
ENV RELAY_POLL_INTERVAL_MS=1000
ENV RELAY_BATCH_SIZE=50

EXPOSE ${PORT}

ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
