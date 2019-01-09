FROM golang:1.11

WORKDIR /app

COPY . .

RUN go build -o app -mod=vendor .

ENTRYPOINT [ "./app", "--brokers=kafka:9092", "--topic=test"]