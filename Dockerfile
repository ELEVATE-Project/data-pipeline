# ---- Base: Ubuntu + JDK + Maven + Scala ----
FROM ubuntu:22.04

# Set timezone & env
ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

# Install required packages
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    tar \
    git \
    curl \
    maven \
    jq \
    nano \
    postgresql-client \
 && rm -rf /var/lib/apt/lists/*

# Install specific Scala version
WORKDIR /usr/local
RUN curl -sL https://downloads.lightbend.com/scala/2.12.11/scala-2.12.11.tgz | tar -xz \
 && mv scala-2.12.11 scala
ENV SCALA_HOME=/usr/local/scala
ENV PATH="${SCALA_HOME}/bin:${PATH}"

# App setup
WORKDIR /app
COPY . /app

# Optional diagnostics
RUN java -version && javac -version && mvn -v && scala -version

# Build project
RUN mvn clean install -DskipTests
