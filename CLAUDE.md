# Coding Practices

- Typical flow for new features:
  1. Add new requirements to the appropriate spec file in `specs/`
  2. Create tests for each new requirement
  3. Write code to make the tests pass
  4. Verify all tests pass before committing

- Specs must have format: `WHEN [condition/event] THE SYSTEM SHALL [expected behavior]`

- Comments should explain motivations and quirks of a block, not repeat what it
  says.

- Never add new dependencies unless explicit permission is given. You always
  have permission to add dependencies from the Go standard library.

# Project Overview

We're building web services for working with OCFL-based repositories

OCFL spec: https://ocfl.io/1.1/spec/
OCFL implementation: github.com/srerickson/ocfl-go

Service's `main` packages are are in `cmd/`.

- `ocfl-webui` for accessing objects in a given OCFL storage root.

# Commands

- `just test` to run all tests
- `just generate` to compile frontend js/css and templ templates. 
- `just ocfl-webui` to start the ocfl-webui service using the testdata storage root.
- `templ fmt .` to format templ templates

# Tools

- templ for html templates
- plain css (DO NOT use tailwindcss)
- htmx and alpinejs for frontend js