# Use Debian as the base image
FROM debian:bullseye-slim

# Install necessary packages, including Docker and its dependencies
RUN apt-get update && apt-get install -y \
    ssh \
    sudo \
    build-essential \
    curl \
    git \
    vim \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release && \
    rm -rf /var/lib/apt/lists/*

# Add Docker's official GPG key and setup stable repository
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update

# Install Docker CLI
RUN apt-get install -y docker-ce docker-ce-cli containerd.io && \
    rm -rf /var/lib/apt/lists/*

# Install Docker Compose
RUN curl -L "https://github.com/docker/compose/releases/download/v2.0.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose && \
    chmod +x /usr/local/bin/docker-compose


# Add a non-root user (optional, but recommended for security)
RUN useradd -m -s /bin/bash myuser && \
    usermod -aG sudo myuser && \
    usermod -aG docker myuser

# Create a directory for your project
WORKDIR /server

# Create the necessary directories
RUN mkdir -p /server/cli \
             # /server/config \
             /server/database \
             /server/scripts \
             /server/services \
             /server/vendors \
             /server/tests

# Copy the project files
COPY ./server/cli /server/cli
# COPY ./server/config /server/config
COPY ./server/database /server/database
COPY ./server/scripts /server/scripts
COPY ./server/services /server/services
COPY ./server/vendors /server/vendors
COPY ./server/tests /server/tests
COPY ./server/.env /server/.env
COPY ./server/docker-compose.yml /server/docker-compose.yml
COPY ./server/Cargo.toml /server/Cargo.toml
COPY ./server/Cargo.lock /server/Cargo.lock
# COPY ./server/tickers_template.json /server/tickers_template.json





