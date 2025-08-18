# This handles both amd64 and arm64 automatically
FROM maven:3.9.9-eclipse-temurin-11 AS build

# Set timezone
ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

# Install extra tools (Postgres client, jq, nano, git, etc.)
RUN apt-get update && apt-get install -y \
      postgresql-client \
      jq \
      nano \
      git \
      curl \
 && rm -rf /var/lib/apt/lists/*

# Install specific Scala version
WORKDIR /usr/local
RUN curl -sL https://downloads.lightbend.com/scala/2.12.11/scala-2.12.11.tgz | tar -xz \
 && mv scala-2.12.11 scala
ENV SCALA_HOME=/usr/local/scala
ENV PATH="${SCALA_HOME}/bin:${PATH}"

# Build stage
WORKDIR /app
COPY . /app

# Optional diagnostics
RUN java -version && javac -version && mvn -v

# Build your project
RUN mvn clean install -DskipTests
