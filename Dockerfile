FROM golang
run git clone https://github.com/sybrexsys/RapidKV.git /go/src/github.com/sybrexsys/RapidKV
run go install github.com/sybrexsys/RapidKV
ENTRYPOINT /go/bin/RapidKV

EXPOSE 18018