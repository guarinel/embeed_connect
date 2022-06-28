FROM eclipse-temurin:11-jre-alpine

COPY target/aws-kpl-producer-jar-with-dependencies.jar app.jar

RUN cat > offset.dat
RUN cat > log.dat

RUN export db_mysql_HOST=database-2.cj12lwz0yxns.us-east-1.rds.amazonaws.com
RUN export db_mysql_PORT=3306
RUN export db_mysql_USER=admin
RUN export db_mysql_PASSWORD=dgp7hbm7

ENTRYPOINT ["java","-jar","app.jar"] 
