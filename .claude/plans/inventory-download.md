# Plan: Add Inventory Download Route

## Summary
Add a route `GET /inventory/{id}` that returns the raw JSON inventory for an OCFL object as a file download, and add a "Download inventory.json" link to the object dropdown menu.

## Files to Modify

1. **`specs/ocfl-webui.md`** - Add spec for inventory download
2. **`access/access.go`** - Add method to read raw inventory
3. **`webui/server.go`** - Add route and handler
4. **`webui/utils/links.go`** - Add link helper function
5. **`webui/template/object_components.templ`** - Add menu item

## Implementation Steps

### 0. Add specification
**File:** `specs/ocfl-webui.md`

Add new section after "Version Changes View":

```markdown
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

WHEN viewing any object page
THE SYSTEM SHALL display a dropdown menu with object actions.

WHEN the object actions menu is displayed
THE SYSTEM SHALL include a "Download inventory.json" link that downloads the object's inventory.
```

### 1. Add `OpenObjectInventory` method to access service
**File:** `access/access.go`

Add a new method that:
- Syncs the object (validates it exists)
- Reads the inventory using `ocfl.ReadInventory()`
- Returns the raw JSON bytes via `MarshalBinary()`

```go
func (s *Service) OpenObjectInventory(ctx context.Context, objID string) ([]byte, error) {
    obj, err := s.SyncObject(ctx, objID)
    if err != nil {
        return nil, err
    }
    inv, err := ocfl.ReadInventory(ctx, s.root.FS(), obj.StoragePath())
    if err != nil {
        return nil, err
    }
    return inv.MarshalBinary()
}
```

### 2. Add route and handler in server.go
**File:** `webui/server.go`

Add route in `New()` function:
```go
mux.HandleFunc("GET /inventory/{id}", HandleGetObjectInventory(accessService))
```

Add handler function:
```go
func HandleGetObjectInventory(svc *access.Service) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        ctx := r.Context()
        objID := r.PathValue("id")

        data, err := svc.OpenObjectInventory(ctx, objID)
        if err != nil {
            if errors.Is(err, access.ErrNotFound) {
                http.Error(w, err.Error(), http.StatusNotFound)
                return
            }
            svc.Logger().LogAttrs(ctx, slog.LevelError, err.Error(),
                slog.String("object_id", objID))
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        w.Header().Set("Content-Type", "application/json")
        w.Header().Set("Content-Disposition", "attachment; filename=\"inventory.json\"")
        w.Header().Set("Content-Length", strconv.Itoa(len(data)))
        w.Write(data)
    }
}
```

### 3. Add link helper function
**File:** `webui/utils/links.go`

```go
func LinkObjectInventory(objID string) templ.SafeURL {
    return templ.URL("/inventory/" + url.PathEscape(objID))
}
```

### 4. Add menu item to dropdown
**File:** `webui/template/object_components.templ`

Add after "Object History" menu item:
```templ
<div class="dropdown-item" role="menuitem">
    <a href={ utils.LinkObjectInventory(objID) }>Download inventory.json</a>
</div>
```

### 5. Regenerate templ files
Run `templ generate` to update `*_templ.go` files.

## Verification

1. Run `just test` to ensure all tests pass
2. Run `just ocfl-webui` to start the server
3. Navigate to an object page and verify:
   - The dropdown menu shows "Download inventory.json"
   - Clicking it downloads a JSON file
   - The file contains the raw OCFL inventory
4. Test the route directly: `curl -I http://localhost:8080/inventory/{object-id}`
   - Verify `Content-Type: application/json`
   - Verify `Content-Disposition: attachment; filename="inventory.json"`
