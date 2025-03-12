# 使用官方 Golang 镜像作为构建环境
FROM golang:1.23 as builder

# 设置工作目录
WORKDIR /app

# 复制 go.mod 和 go.sum 文件
COPY go.mod go.sum* ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux go build -v -o server

# 使用 Google 的 distroless 作为生产环境
FROM gcr.io/distroless/base-debian11

# 复制构建的应用
COPY --from=builder /app/server /server

# 复制 server-ca.pem 证书文件
COPY --from=builder /app/server-ca.pem /server-ca.pem
# 暴露端口
EXPOSE 8080

# 运行应用
CMD ["/server"]