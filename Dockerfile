FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
VOLUME /data/kafka-backup
COPY target/kafka-s3-backup-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
