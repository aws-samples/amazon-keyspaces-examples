FROM node:bullseye-slim
WORKDIR /usr/src/app
COPY package*.json ./
ADD https://certs.secureserver.net/repository/sf-class2-root.crt ./
RUN npm install
COPY . .
EXPOSE 80
CMD ["node", "query-api-server.js"]
