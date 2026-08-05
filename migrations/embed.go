// Package migrations 嵌入版本化 SQL，供 cmd/migrate 使用（GOCHAT_DATABASE.md §2.4）。
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
