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
$ docker run --rm -p 8283:8283 \
    -v $(pwd)/testdata/reg-extension-dir-root:/data \
    -e OCFL_ROOT=/data \
    ghcr.io/srerickson/ocfl-webui

# browse object: http://localhost:8283/object/ark%3A123%2Fabc/head/
# or download its content
$ curl -o - http://localhost:8283/object/ark%3A123%2Fabc/head/a_file.txt
> Hello! I am a file.
```

The `OCFL_ROOT` environment variable should be set to the OCFL storage root to
serve. The value can be a local filesystem path or an S3 bucket using the format
`s3://bucket/prefix`. The storage root must use a recognized storage layout.

Here is an example for using a non-AWS S3 endpoint:

```sh
export OCFL_ROOT=s3://bucket/root
export AWS_REGION=us-xyx
export AWS_ENDPOINT_URL=https://s3.us-xyz.s3provider.com
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# pass environment variables into the container
docker run --rm -p 8283:8283 \
        -e OCFL_ROOT \
        -e AWS_REGION \
        -e AWS_ENDPOINT_URL \
        -e AWS_ACCESS_KEY_ID \
        -e AWS_SECRET_ACCESS_KEY \
        ghcr.io/srerickson/ocfl-webui
```

The credentials must have permissions for S3 actions:
- `HeadObject`
- `GetObject`
- `ListObjectsV2`