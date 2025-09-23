FROM node:20.12.2-alpine

RUN apk update && apk upgrade && \
    apk add --no-cache bash ffmpeg nano

WORKDIR /app

# Copy dependency files and install (including devDependencies for build)
COPY package*.json .
RUN npm install

COPY ./index.js .

# Copy the full project and build TypeScript
COPY . .
RUN npm run build && ls -la dist

CMD ["node", "index.js"]

