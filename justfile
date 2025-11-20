devtools:
    npm install
    go install github.com/a-h/templ/cmd/templ@latest
    go install github.com/google/ko@latest

assets:
    npm run build:css
    npm run build:js

webui:
    templ generate --watch --proxy="http://localhost:8090" --cmd="go run ./cmd/ocfl-webui/main.go -root testdata/reg-extension-dir-root" --open-browser=false

build:
    KO_DOCKER_REPO=ghcr.io/srerickson ko build ./cmd/ocfl-webui --base-import-paths