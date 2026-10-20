FROM node:24-alpine

WORKDIR /app

ENV NODE_ENV=production

COPY package.json package-lock.json ./

RUN npm ci --omit=dev

COPY --chown=node:node src ./src

USER node

EXPOSE 5000

CMD ["node", "src/server.js"]