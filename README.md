# OCFL Web Services

I hope to populate this repository with a bunch of interesting web services for
accessing and managing [OCFL](https://ocfl.io)-based repositories. For now,
there is just one.

## Services

### `ocfl-webui`

A simple web server for browsing and downloading files from objects in a storage
root. It's distributed as container image: `ghcr.io/srerickson/ocfl-webui`

```sh
# using the testdata storage root
$ docker run --rm -d \
    -p 8283:8283 \
    -v $(pwd)/testdata/reg-extension-dir-root:/data \
    -e OCFL_ROOT=/data \
    ghcr.io/srerickson/ocfl-webui

# browse ark:123/abc: http://localhost:8283/object/ark%3A123%2Fabc/head/
# or download its content
$ curl -o - http://localhost:8283/object/ark%3A123%2Fabc/head/a_file.txt
> Hello! I am a file.
```