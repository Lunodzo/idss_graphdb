FROM golang:1.27.1-bookworm AS builder

WORKDIR /src

COPY go.mod ./
COPY vendor ./vendor
COPY broadcast ./broadcast
COPY client ./client
COPY common ./common
COPY flags ./flags
COPY helpers ./helpers
COPY kaddht ./kaddht
COPY server/*.go ./server/
ENV GOTOOLCHAIN=local

RUN CGO_ENABLED=0 go build -mod=vendor -o /out/idss-server ./server && \
    CGO_ENABLED=0 go build -mod=vendor -o /out/idss-client ./client

FROM python:3.13-slim-bookworm

WORKDIR /app/server

RUN pip install --no-cache-dir faker

COPY --from=builder /out/idss-server ./idss-server
COPY --from=builder /out/idss-client /app/client/idss-client
COPY server/generate_data.py ./generate_data.py

ENTRYPOINT ["./idss-server"]