FROM openjdk:8-jdk-alpine

# Install required packages for installation
RUN apk add --no-cache \
    bash \
    su-exec \
    python

ARG atlas-tar

ADD "$atlas-tar" /atlas

WORKDIR /atlas

EXPOSE 21000




