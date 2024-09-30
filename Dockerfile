FROM --platform=linux/amd64 node:20-alpine
RUN mkdir -p /opt/job-service
COPY . ./opt/job-service
WORKDIR /opt/job-service
RUN npm install
COPY . .
CMD ["npm", "run", "start"]