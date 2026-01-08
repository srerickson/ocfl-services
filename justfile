test: generate
    go test ./...

# generate static assets (js/css) and templ templates
generate: install-tools
    npx esbuild ./webui/static/src/styles/app.css ./webui/static/src/app.js --bundle --minify --outdir=./webui/static/dst --entry-names=[name]
    templ generate

# serve webui server http://localhost:8284
ocfl-webui: generate
    OCFL_ROOT=testdata/reg-extension-dir-root \
        go run ./cmd/ocfl-webui

ocfl-webui-live: install-tools
    OCFL_ROOT=testdata/reg-extension-dir-root \
        air -c .air.toml

build: test
    KO_DOCKER_REPO=ghcr.io/srerickson ko build ./cmd/ocfl-webui --base-import-paths

install-tools:
    npm install
    go install github.com/a-h/templ/cmd/templ@v0.3.960
    go install github.com/google/ko@v0.18.0
    go install github.com/air-verse/air@latest
