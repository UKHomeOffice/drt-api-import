FROM openjdk:alpine
WORKDIR /opt/docker
ADD target/docker/stage/opt /opt
RUN adduser -D -u 1000 drt

RUN ["chown", "-R", "1000:1000", "."]

RUN apk --update add bash less curl
RUN rm -rf /var/cache/apk/*

RUN mkdir -p /home/drt/.postgresql
RUN curl https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem > /home/drt/.postgresql/root.crt


USER 1000

ENTRYPOINT ["bin/drt-api-import"]
