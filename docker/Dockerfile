# syntax=docker/dockerfile:1

FROM golang:1.20

RUN apt-get update && apt-get install -yq --no-install-recommends libzmq3-dev

# Set destination for COPY
WORKDIR /zmq2mqtt

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code. Note the slash at the end, as explained in
# https://docs.docker.com/engine/reference/builder/#copy
COPY *.go ./

# Build
RUN GOOS=linux go build -o /zmq2mqtt

# Optional:
# To bind to a TCP port, runtime parameters must be supplied to the docker command.
# But we can document in the Dockerfile what ports
# the application is going to listen on by default.
# https://docs.docker.com/engine/reference/builder/#expose
EXPOSE 1883

# Run
CMD ["/zmq2mqtt"]