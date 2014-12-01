cur_path=`pwd`
export GOPATH=$cur_path:$GOPATH

go build -o bin/gotair src/main.go
