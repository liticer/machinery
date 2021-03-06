## Logging

A simple leveled logging library with coloured output.

[![Travis Status for liticer/logging](https://travis-ci.org/liticer/logging.svg?branch=master&label=linux+build)](https://travis-ci.org/liticer/logging)
[![godoc for liticer/logging](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/liticer/logging)
[![codecov for liticer/logging](https://codecov.io/gh/liticer/logging/branch/master/graph/badge.svg)](https://codecov.io/gh/liticer/logging)
[![Codeship Status for liticer/logging](https://app.codeship.com/projects/3844b520-3c97-0134-92d6-6a89aa757e0d/status?branch=master)](https://app.codeship.com/projects/166987)

[![Sourcegraph for liticer/logging](https://sourcegraph.com/github.com/liticer/logging/-/badge.svg)](https://sourcegraph.com/github.com/liticer/logging?badge)
[![Donate Bitcoin](https://img.shields.io/badge/donate-bitcoin-orange.svg)](https://richardknop.github.io/donate/)

---

Log levels:

- `INFO` (blue)
- `WARNING` (pink)
- `ERROR` (red)
- `FATAL` (red)

Formatters:

- `DefaultFormatter`
- `ColouredFormatter`

Example usage. Create a new package `log` in your app such that:

```go
package log

import (
	"github.com/liticer/logging"
)

var (
	logger = logging.New(nil, nil, new(logging.ColouredFormatter))

	// INFO ...
	INFO = logger[logging.INFO]
	// WARNING ...
	WARNING = logger[logging.WARNING]
	// ERROR ...
	ERROR = logger[logging.ERROR]
	// FATAL ...
	FATAL = logger[logging.FATAL]
)
```

Then from your app you could do:

```go
package main

import (
	"github.com/yourusername/yourapp/log"
)

func main() {
	log.INFO.Print("log message")
}
```
