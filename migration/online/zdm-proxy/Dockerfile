FROM datastax/zdm-proxy:latest

# Install tools
RUN apk add --no-cache socat bind-tools curl openssl

RUN mkdir -p /app

# Download Amazon Root CA 1 for TLS verification with Keyspaces VPC endpoints
ADD https://www.amazontrust.com/repository/AmazonRootCA1.pem /app/AmazonRootCA1.pem

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
