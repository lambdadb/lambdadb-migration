FROM golang:1.26-alpine AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=none

RUN apk add --no-cache ca-certificates git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/lambdadb-migration .

FROM alpine:3.23

RUN apk add --no-cache ca-certificates
COPY --from=build /out/lambdadb-migration /usr/local/bin/lambdadb-migration

USER 65532:65532
ENTRYPOINT ["lambdadb-migration"]
