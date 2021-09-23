FROM golang:alpine as build

WORKDIR /build
COPY . ./
RUN CGO_ENABLED=0 go build -v -o dbload

FROM alpine:latest
WORKDIR /root/
COPY --from=build /build/dbload ./

CMD ["./dbload"]
