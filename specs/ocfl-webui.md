# Requirements for ocfl-webui

## Homepage

WHEN an http client requests `/`
THE SYSTEM SHALL responds with a form for looking-up objects by ID.

WHEN a user enters an object id and clicks the form submit button on the homepage form,
THE SYSTEM SHALL url-encode the ID and redirect the user to `/object/{object_id}/head/`

## Static Assets

WHEN an http client requests `/static/{path}`
THE SYSTEM SHALL serve static files (CSS, JS) from the embedded static/dst directory.

## Object Files View

WHEN an http client requests `/object/{object_id}`
THE SYSTEM SHALL redirect to `/object/{object_id}/head/`.

WHEN an http client requests `/object/{object_id}/`
THE SYSTEM SHALL redirect to `/object/{object_id}/head/`.

WHEN an http client requests `/object/{object_id}/{version}` (without trailing slash)
THE SYSTEM SHALL redirect to `/object/{object_id}/{version}/`.

WHEN an http client requests `/object/{object_id}/{version}/`
THE SYSTEM SHALL check that the object exists and respond with HTML listing files in the top-level directory of the given object version state.

WHEN an http client requests `/object/{object_id}/{version}/{path}/` (directory path with trailing slash)
THE SYSTEM SHALL respond with HTML listing files in the given directory of the given object version state.

WHEN an http client requests `/object/{object_id}/{version}/{path}` (file path without trailing slash)
THE SYSTEM SHALL respond with the file content for download.

### Version References

WHEN an http client requests an object with version reference "head"
THE SYSTEM SHALL resolve "head" to the most recent version.

WHEN an http client requests an object with a version number in OCFL format (e.g., "v1", "v002")
THE SYSTEM SHALL resolve the version number to the corresponding object version.

WHEN an http client requests an object with an invalid version format
THE SYSTEM SHALL respond with HTTP 400 Bad Request.

### Directory Listing Behavior

WHEN listing a directory that is not the root
THE SYSTEM SHALL include a parent directory entry ("..") linking to the parent directory.

WHEN listing a directory
THE SYSTEM SHALL sort directory entries with directories appearing before files, and alphabetically within each group.

WHEN a directory contains a file named "readme.md" or "readme.txt" (case-insensitive)
THE SYSTEM SHALL include a link to render the README content in the response.

### File Download Behavior

WHEN serving a file for download
THE SYSTEM SHALL set the Content-Length header to the file size.

WHEN an http client sends a HEAD request for a file
THE SYSTEM SHALL respond with headers only (no body).

### README Rendering

WHEN an http client requests `/object/{object_id}/{version}/{path}?render=1` for a README file
THE SYSTEM SHALL render the markdown content as HTML.

WHEN an http client requests `?render=1` for a file that is not named "readme.md" or "readme.txt" (case-insensitive)
THE SYSTEM SHALL respond with HTTP 400 Bad Request.

WHEN an http client requests `?render=1` for a markdown file larger than 2 MiB
THE SYSTEM SHALL not render the file.

## Object History View

WHEN an http client requests `/history/{object_id}`
THE SYSTEM SHALL respond with HTML listing all versions of the object in reverse chronological order (most recent first), displaying version metadata including: version number, created timestamp, message, user name, and user address.

## Version Changes View

WHEN an http client requests `/history/{object_id}/{version}`
THE SYSTEM SHALL respond with HTML showing the file changes introduced in that version as a hierarchical file tree structure, indicating the modification type for each file (added, modified, deleted).

WHEN displaying a version changes file tree
THE SYSTEM SHALL sort entries with directories before files, alphabetically within each group.

WHEN viewing a version that is not the most recent
THE SYSTEM SHALL provide navigation to the next version.

WHEN viewing a version that is not the first version
THE SYSTEM SHALL provide navigation to the previous version.

WHEN an http client requests `/history/{object_id}/{version}` with an invalid version format
THE SYSTEM SHALL respond with HTTP 400 Bad Request.

WHEN an http client requests `/history/{object_id}/{version}` for a version that does not exist
THE SYSTEM SHALL respond with HTTP 404 Not Found.

## Error Handling

WHEN an object is not found
THE SYSTEM SHALL respond with HTTP 404 Not Found.

WHEN a file or directory path is not found within an object
THE SYSTEM SHALL respond with HTTP 404 Not Found.

WHEN an internal error occurs
THE SYSTEM SHALL respond with HTTP 500 Internal Server Error and log the error with context.

## Logging

WHEN an http request is received
THE SYSTEM SHALL log the request via logging middleware.

WHEN an error occurs during request handling
THE SYSTEM SHALL log the error with relevant context (object_id, path, version, etc.).


## Inventory Download

WHEN an http client requests `/inventory/{object_id}`
THE SYSTEM SHALL respond with the raw JSON content from the object's root inventory.json file.

WHEN serving an inventory download
THE SYSTEM SHALL set the Content-Type header to "application/json".

WHEN serving an inventory download
THE SYSTEM SHALL set the Content-Disposition header to trigger a file download with filename "inventory.json".

WHEN an http client requests `/inventory/{object_id}` for an object that does not exist
THE SYSTEM SHALL respond with HTTP 404 Not Found.

## Object Actions Menu

WHEN viewing any object page (`/object/...` or `/history/....`)
THE SYSTEM SHALL display a dropdown menu with object actions.

WHEN the object actions menu is displayed
THE SYSTEM SHALL include a "Download inventory.json" link that downloads the object's inventory.

## Authentication

WHEN OAuth is configured and a user clicks the login button
THE SYSTEM SHALL redirect the user to Google OAuth for authentication.

WHEN a user completes the OAuth login flow successfully
THE SYSTEM SHALL redirect the user back to the page where they initiated the login.

WHEN a user is authenticated
THE SYSTEM SHALL display their name and avatar in the header with a logout button.

WHEN a user is not authenticated
THE SYSTEM SHALL display a "Login with Google" button in the header.

WHEN OAuth is configured and an unauthenticated user attempts to download a file
THE SYSTEM SHALL respond with HTTP 401 Unauthorized.
