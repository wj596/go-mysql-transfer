FROM golang:1.14 as compiler

ENV GO111MODULE=on \
    GOPROXY=https://goproxy.cn,direct

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o transfer .

RUN mkdir publish && cp transfer publish && \
    cp app.yml publish && cp -r web/statics publish

# 第二阶段
FROM alpine

WORKDIR /app

COPY --from=compiler /app/publish .

# 注意修改端口
EXPOSE 8060

ENTRYPOINT ["./transfer"]