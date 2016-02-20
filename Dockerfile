FROM golang

RUN go get "github.com/gorilla/mux" && \
    go get "github.com/lib/pq" && \
    go get "gopkg.in/yaml.v2"


#RUN cd /go/src/autoscope/ && go get -d -v

ENV GOBIN /go/bin/
ENV GOPATH /go/
ENV GOSRC /go/src/
ENV PATH $GOBIN:$PATH:$GOSRC

RUN mkdir /go/src/autoscope/
WORKDIR /go/src/autoscope/
COPY ./docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]

COPY ./autoscope.go /go/src/autoscope/
COPY ./server/ /go/src/autoscope/server/
COPY ./engine/ /go/src/autoscope/engine/

WORKDIR /go/src/autoscope/
RUN go install /go/src/autoscope/autoscope.go

EXPOSE 4210
