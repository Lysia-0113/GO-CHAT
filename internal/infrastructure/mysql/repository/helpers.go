package repository

import (
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

// uuidToBytes 将 UUID 字符串转为 BINARY(16) 字节（GOCHAT_DATABASE.md §7.3）。
func uuidToBytes(s string) ([]byte, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return nil, err
	}
	b, err := u.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return b, nil
}

// bytesToUUID 将 BINARY(16) 转回 UUID 字符串。
func bytesToUUID(b []byte) string {
	u, err := uuid.FromBytes(b)
	if err != nil {
		return ""
	}
	return u.String()
}

// lockForUpdateClause 返回 SELECT ... FOR UPDATE 子句（会话 seq 串行化）。
func lockForUpdateClause() clause.Locking {
	return clause.Locking{Strength: "UPDATE"}
}
