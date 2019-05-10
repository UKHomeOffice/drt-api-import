FROM openjdk:alpine
WORKDIR /opt/docker
ADD target/docker/stage/opt /opt
RUN adduser -D -u 1000 drt-admin

RUN ["chown", "-R", "1000:1000", "."]

RUN apk --update add bash less
RUN rm -rf /var/cache/apk/*

RUN keytool -noprompt -storepass changeit -import -alias rds -keystore $JAVA_HOME/jre/lib/security/cacerts -file certs/rds-combined-ca-bundle.pem

USER 1000

ENTRYPOINT ["bin/drt-api-import"]
