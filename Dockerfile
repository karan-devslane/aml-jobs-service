FROM --platform=linux/amd64 node:20-alpine
RUN apk add --no-cache postgresql-client
RUN mkdir -p /opt/job-service
COPY . ./opt/job-service
WORKDIR /opt/job-service
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "run", "start"]