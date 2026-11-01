FROM gradle:8.10.2-jdk21 AS build
WORKDIR /src
COPY settings.gradle build.gradle ./
COPY src src
RUN gradle installDist --no-daemon -q

FROM eclipse-temurin:21-jre
COPY --from=build /src/build/install/cleaner /app
# Container limit is 256 MB: cap heap, metaspace and code cache; SerialGC has the smallest footprint.
ENV JAVA_OPTS="-Xmx192m -XX:MaxMetaspaceSize=64m -XX:ReservedCodeCacheSize=32m -Xss512k -XX:+UseSerialGC"
ENTRYPOINT ["/app/bin/cleaner"]
