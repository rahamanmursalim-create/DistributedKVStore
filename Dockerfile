# Build the Java application with Maven and Java 17
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -B clean package -DskipTests

# Run the packaged application on a small Java 17 runtime
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/redis-java-clone-1.0.0.jar /app/app.jar

# Render provides PORT for HTTP traffic. DATA_DIR can be overridden in Render.
ENV PORT=8080
ENV TCP_PORT=6379
ENV DATA_DIR=/var/data
RUN mkdir -p /var/data && useradd --system --uid 10001 appuser && chown -R appuser:appuser /app /var/data
USER appuser
EXPOSE 8080 6379
CMD ["java", "-jar", "/app/app.jar"]
