# OCFL Services

Web services for working with OCFL-based repositories

OCFL spec: https://ocfl.io/1.1/spec/
OCFL implementation: github.com/srerickson/ocfl-go

# Overview

Service entrypoints are defined in `cmd/`.

- `ocfl-webui` for accessing objects in a given OCFL storage root.

# Commands

- `just test` to run all tests
- `just generate` to compile frontend js/css and templ templates. 
- `just ocfl-webui` to start the ocfl-webui service using the testdata storage root.
- `templ fmt .` to format templ templates


# Tools

- templ for html template
- plain css (DO NOT use tailwindcss)
- htmx and alpinejs for frontend js