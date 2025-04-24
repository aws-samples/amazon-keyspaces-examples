FROM datastax/zdm-proxy:latest

# Install tools
RUN apk add --no-cache socat bind-tools curl

RUN mkdir -p /app

ADD https://certs.secureserver.net/repository/sf-class2-root.crt /app/sf-class2-root.crt

# Declare build-time ARG
#ARG ARG_KEYSPACES_ENPOINT=cassandra.us-east-1.amazonaws.com

# Make it available at runtime via ENV
# ENV KEYSPACES_ENPOINT=${ARG_KEYSPACES_ENPOINT

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
