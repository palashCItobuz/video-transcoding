FROM node:20.12.2-alpine as base

RUN apk update && apk upgrade && \
    apk add --no-cache bash ffmpeg nano

WORKDIR /app

# Copy dependency files and install (including devDependencies for build)
COPY package*.json .

RUN npm install

COPY index.js .

CMD ["npm", "start"]

