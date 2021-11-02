FROM node:bullseye-slim
WORKDIR /usr/src/app
COPY package*.json ./
ADD https://certs.secureserver.net/repository/sf-class2-root.crt ./
RUN npm install
COPY . .
CMD ["node", "load-data.js"]
