# Use official Go image as base (Go 1.25)
FROM mcr.microsoft.com/devcontainers/go:dev-1.25

# Install Node.js 24.x (latest LTS) and npm
RUN curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && sudo apt-get update \
    && sudo apt-get install -y \
        nodejs \
        just \
    && sudo apt-get clean