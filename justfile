# run all tests
test: generate
    go test ./...

# run all codegen for the project: css/js and templ tempaltes
generate:
    npx esbuild ./webui/static/src/styles/app.css ./webui/static/src/app.js --bundle --minify --outdir=./webui/static/dst --entry-names=[name]
    templ generate

# start ocfl-webui http://localhost:8284
ocfl-webui: generate
    OCFL_ROOT=testdata/reg-extension-dir-root \
        go run ./cmd/ocfl-webui

# start ocfl-webui using air (with live reloading)
ocfl-webui-live:
    OCFL_ROOT=testdata/reg-extension-dir-root \
        air -c .air.toml

# build the container
build: test
    KO_DOCKER_REPO=ghcr.io/srerickson ko build ./cmd/ocfl-webui --base-import-paths

install-tools:
    npm install
    go install github.com/a-h/templ/cmd/templ@v0.3.960
    go install github.com/google/ko@v0.18.0
    go install github.com/air-verse/air@latest
