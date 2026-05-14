FROM gradle:8.7-jdk21 AS builder

WORKDIR /src
COPY settings.gradle.kts build.gradle.kts ./
RUN gradle --no-daemon --refresh-dependencies dependencies || true

COPY src ./src
RUN gradle --no-daemon build -x test \
    && cp build/libs/processor-*.jar /out.jar 2>/dev/null \
    || (ls build/libs && cp build/libs/*.jar /out.jar)

FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
COPY --from=builder /out.jar /app/processor.jar
COPY src/main/resources/application.yml /app/application.yml
RUN addgroup -S app && adduser -S -G app app && chown -R app:app /app
USER app
ENTRYPOINT ["java", "-jar", "/app/processor.jar"]
CMD ["/app/application.yml"]
