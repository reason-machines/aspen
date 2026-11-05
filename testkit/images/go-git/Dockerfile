# go-git v5 (pinned in go.mod), for the client matrix.
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /aspen-scenario .

FROM debian:bookworm-slim
COPY --from=build /aspen-scenario /usr/local/bin/aspen-scenario
ENTRYPOINT ["aspen-scenario"]
