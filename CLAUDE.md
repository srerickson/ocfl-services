# Coding Practices

- Functionality is defined in specs (`specs/`); specs are used to create tests;
  tests are used to generate and improve implementations of the functionality.

- Specs have format: `WHEN [condition/event] THE SYSTEM SHALL [expected behavior]`

## Preferred Flow for New Features

1. **Update the spec** - Add new requirements to the appropriate spec file in `specs/`
2. **Write tests** - Create tests based on the new requirements
3. **Implement** - Write the code to make the tests pass
4. **Run tests** - Verify all tests pass before committing

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

- templ for html template
- plain css (DO NOT use tailwindcss)
- htmx and alpinejs for frontend js