FROM eclipse-temurin:11-jre-alpine

COPY target/aws-kpl-producer-jar-with-dependencies.jar app.jar

RUN cat > offset.dat
RUN cat > log.dat

ENTRYPOINT ["java","-jar","app.jar"] 
