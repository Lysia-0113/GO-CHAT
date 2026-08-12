// Command migrate 按顺序执行 migrations/ 下的版本化 SQL（GOCHAT_DATABASE.md §2.4）。
//
// 用法：
//
//	GOChat_MYSQL_DSN="..." go run ./cmd/migrate
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/Lysia-0113/GO-CHAT/internal/config"
	"github.com/Lysia-0113/GO-CHAT/migrations"
)

func main() {
	dsn := flag.String("dsn", envOr("GOChat_MYSQL_DSN", ""), "MySQL DSN")
	flag.Parse()
	if *dsn == "" {
		cfg, err := config.LoadConfig(envOr("GOChat_CONFIG", "./config/config.yaml"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "需要提供 -dsn 或 GOChat_MYSQL_DSN 环境变量")
			os.Exit(1)
		}
		*dsn = cfg.MySQL.DSN
	}
	// migration 文件含多语句，需要 multiStatements
	if !strings.Contains(*dsn, "multiStatements") {
		sep := "?"
		if strings.Contains(*dsn, "?") {
			sep = "&"
		}
		*dsn += sep + "multiStatements=true"
	}

	db, err := gorm.Open(mysql.Open(*dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "连接 MySQL 失败:", err)
		os.Exit(1)
	}
	sqlDB, err := db.DB()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sqlDB.SetConnMaxLifetime(time.Minute)
	defer sqlDB.Close()

	if err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version VARCHAR(64) PRIMARY KEY,
		applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
	) ENGINE = InnoDB`).Error; err != nil {
		fmt.Fprintln(os.Stderr, "创建 schema_migrations 失败:", err)
		os.Exit(1)
	}

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, f := range files {
		var count int64
		db.Table("schema_migrations").Where("version = ?", f).Count(&count)
		if count > 0 {
			fmt.Println("skip (applied):", f)
			continue
		}
		b, err := migrations.FS.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, "读取失败:", f, err)
			os.Exit(1)
		}
		if err := db.Exec(string(b)).Error; err != nil {
			fmt.Fprintln(os.Stderr, "执行失败:", f, err)
			os.Exit(1)
		}
		if err := db.Exec("INSERT INTO schema_migrations (version) VALUES (?)", f).Error; err != nil {
			fmt.Fprintln(os.Stderr, "记录版本失败:", f, err)
			os.Exit(1)
		}
		fmt.Println("applied:", f)
	}
	fmt.Println("migrate done")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
