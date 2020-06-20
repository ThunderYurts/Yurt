FROM golang:latest as builder
WORKDIR /code
ADD . /code
ENV GOPROXY https://goproxy.cn
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .

FROM centos as prod
EXPOSE 8080 8000
WORKDIR /root/
COPY --from=0 /code/app .
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]