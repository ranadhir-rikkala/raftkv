FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM alpine:3.20
COPY --from=build /out/raftkv-server /out/raftkv-cli /usr/local/bin/
VOLUME /data
EXPOSE 7000
ENTRYPOINT ["raftkv-server"]
