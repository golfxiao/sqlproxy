package backend

import (
	"context"
	"sqlproxy/core/golog"
)

const (
	CTX_KEY_CONVERTER = "CONVERTER"
)

type IContext interface {
	// 设置业务上下文
	WithContext(context.Context)
	// 返回上下文信息
	GetContext() context.Context
}

type Context struct {
	ctx context.Context
}

func NewContext(ctx context.Context) Context {
	c := Context{}
	c.WithContext(ctx)
	return c
}

// 返回上下文
func (c *Context) GetContext() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	golog.Debug("Context", "WithContext", "not found ctx, return background", 0)
	return context.Background()
}

// 更新上下文
func (c *Context) WithContext(ctx context.Context) {
	if ctx == nil {
		golog.Warn("Context", "WithContext", "Set context failed, param of ctx nil", 0)
		return
	}
	c.ctx = ctx
}
