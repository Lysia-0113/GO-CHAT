// Package idgen 定义业务侧最小发号接口。
// 业务代码只依赖 Next(ctx)；biz_tag 在装配阶段绑定（GOCHAT_API.md §12.8）。
package idgen

import "context"

// IDGenerator 是业务侧唯一依赖的发号接口。
type IDGenerator interface {
	// Next 返回下一个全局唯一 ID。
	// current 与 next 号段耗尽且无法申请新号段时返回 ID_GENERATOR_UNAVAILABLE。
	Next(ctx context.Context) (int64, error)
}

// Generator 为具体 biz_tag 绑定的生成器。
type Generator struct {
	BizTag string
	Gen    IDGenerator
}

// Next 转发给内部生成器。
func (g *Generator) Next(ctx context.Context) (int64, error) { return g.Gen.Next(ctx) }
