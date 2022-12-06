FROM node:bullseye-slim
WORKDIR /usr/src/app
COPY package.json *.js ./
RUN npm install
ADD https://certs.secureserver.net/repository/sf-class2-root.crt ./
CMD ["node", "load-data.js"]
