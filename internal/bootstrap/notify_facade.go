package bootstrap

// notifyFacade combines app/notify.Admin (CRUD) and app/notify.Service
// (Test delivery) into one type so the REST handlers see a single
// dependency. Separate concerns inside app/notify; flat surface
// outside.

import (
	"context"

	appnotify "github.com/jaenster/hoardarr/internal/app/notify"
	"github.com/jaenster/hoardarr/internal/domain/notify"
)

type notifyFacade struct {
	admin *appnotify.Admin
	svc   *appnotify.Service
}

func (f *notifyFacade) List(ctx context.Context) ([]*notify.Subscription, error) {
	return f.admin.List(ctx)
}
func (f *notifyFacade) Add(ctx context.Context, cmd appnotify.AddCmd) (notify.SubscriptionID, error) {
	id, err := f.admin.Add(ctx, cmd)
	if err == nil {
		_ = f.svc.RefreshCache(ctx)
	}
	return id, err
}
func (f *notifyFacade) Update(ctx context.Context, id notify.SubscriptionID, cmd appnotify.UpdateCmd) error {
	err := f.admin.Update(ctx, id, cmd)
	if err == nil {
		_ = f.svc.RefreshCache(ctx)
	}
	return err
}
func (f *notifyFacade) Remove(ctx context.Context, id notify.SubscriptionID) error {
	err := f.admin.Remove(ctx, id)
	if err == nil {
		_ = f.svc.RefreshCache(ctx)
	}
	return err
}
func (f *notifyFacade) SetEnabled(ctx context.Context, id notify.SubscriptionID, enabled bool) error {
	err := f.admin.SetEnabled(ctx, id, enabled)
	if err == nil {
		_ = f.svc.RefreshCache(ctx)
	}
	return err
}
func (f *notifyFacade) Test(ctx context.Context, id notify.SubscriptionID) error {
	return f.svc.Test(ctx, id)
}
