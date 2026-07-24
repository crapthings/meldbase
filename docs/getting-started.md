# Five-minute Go tour

This is the shortest path to a durable Meldbase database. It creates one file,
writes a todo, reads it back, and leaves the file on disk for the next run.

```sh
mkdir meldbase-tour && cd meldbase-tour
go mod init example.com/meldbase-tour
go get github.com/crapthings/meldbase@latest
```

Create `main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/crapthings/meldbase"
)

func main() {
	ctx := context.Background()
	db, err := meldbase.Open("todos.meld")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	todos := db.Collection("todos")
	_, err = todos.InsertOne(ctx, meldbase.Document{
		"title": meldbase.String("Read the docs"),
		"done":  meldbase.Bool(false),
	})
	if err != nil {
		log.Fatal(err)
	}

	cursor, err := todos.Find(ctx, meldbase.Filter{"done": false})
	if err != nil {
		log.Fatal(err)
	}
	open, err := cursor.All(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("open todos:", len(open))
}
```

Run it:

```sh
go run .
```

You should see `open todos: 1`. Run it again and the count grows: `todos.meld`
is the durable database file.

## Next step

Add an index before a query needs to be fast at scale. Run `meld serve` when a
browser or another process needs access to the same file. Read the
[Browser guide](/browser) or [Node.js guide](/node) for that remote client path.

Use `meldbase.New()` instead of `meldbase.Open(path)` only when in-memory data
is enough. One writable process may open a durable file at a time.
