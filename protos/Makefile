.PHONY: go

dirs = core serving types storage
service_dirs = core serving

gen-go:
	@$(foreach dir,$(dirs),protoc -I/usr/local/include -I. --go_out=plugins=grpc,paths=source_relative:../sdk/go/protos/ feast/$(dir)/*.proto;)

gen-python:
	pip install grpcio-tools
	pip install mypy-protobuf
	@$(foreach dir,$(dirs),python -m grpc_tools.protoc -I. --python_out=../sdk/python/ --mypy_out=../sdk/python/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(service_dirs),python -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/ feast/$(dir)/*.proto;)

install-dependencies-docs:
	go get github.com/golang/protobuf/proto && \
	go get gopkg.in/russross/blackfriday.v2 && \
	cd $$(mktemp -d) && \
	git clone https://github.com/istio/tools/ && \
	cd tools/cmd/protoc-gen-docs && \
	go build && \
	cp protoc-gen-docs $$HOME/bin && \
	export PATH=$$HOME/bin:$$PATH

gen-docs:
	protoc --docs_out=../docs/api/ feast/*/*.proto || \
	$(MAKE) install-dependencies-docs && protoc --docs_out=../dist/grpc feast/*/*.proto