# generate static assets (js/css) and templ templates
generate:
    npx esbuild ./webui/static/src/* --bundle --minify --outdir=./webui/static/dst
    templ generate

# live webui server that reloads on file changes (port 8284)
live:
    air -c .air.toml

build:
    KO_DOCKER_REPO=ghcr.io/srerickson ko build ./cmd/ocfl-webui --base-import-paths

install-tools:
    go install github.com/a-h/templ/cmd/templ@latest
    go install github.com/google/ko@latest
    go install github.com/air-verse/air@latest
