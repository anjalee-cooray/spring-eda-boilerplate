FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

COPY build/libs/*.jar app.jar

ENV DB_URL=""
ENV DB_USERNAME=""
ENV DB_PASSWORD=""

ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
