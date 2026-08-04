

To compile this proto file for go -- there is a compiled version already, this is only needed if changes are made.

1. installed protocol buffers https://protobuf.dev/downloads/
2. Followed https://protobuf.dev/getting-started/gotutorial/#compiling-protocol-buffers 

    * `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
    * made sure the go bin folder was on my `$PATH`

3. Compiled by running the following from the root of the project:

        ~/bin/protoc-31.1-linux-x86_64/bin/protoc --go_out=./ ./types/partitioning.proto


To compile this proto file for Python -- there is a compiled version already, this is only needed if changes are made.

1. Installed protocol buffers https://protobuf.dev/downloads/
2. (Optional) Installed mypy-protobuf https://pypi.org/project/mypy-protobuf/
    * This helps with linting / syntax-highlighting when developing
3. Compiled by running the following from the root of the project (if step 2 is skipped, ignore the `--mypy_out` argument)

        ~/bin/protoc-31.1-linux-x86_64/bin/protoc --python_out=./summary_trainer/ --mypy_out=./summary_trainer/ ./types/partitioning.proto
        mv ./summary_trainer/types/ ./summary_trainer/src/summary_trainer/pb

