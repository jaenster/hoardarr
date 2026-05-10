package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

func newServerRepo(t *testing.T) (*ServerRepo, *DB) {
	t.Helper()
	db := openMigratedDB(t)
	return NewServerRepo(db), db
}

func mustAdd(t *testing.T, repo *ServerRepo, name string, port int, priority int) *server.UsenetServer {
	t.Helper()
	s, err := server.New(server.NewParams{
		Name: name, Host: "host." + name, Port: port,
		Username: "u", Password: "p", MaxConns: 10, Priority: priority,
	}, time.UnixMilli(1).UTC())
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := repo.Save(context.Background(), s); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return s
}

func TestServerRepo_SaveInsertAssignsID(t *testing.T) {
	repo, _ := newServerRepo(t)
	s := mustAdd(t, repo, "main", 563, 0)
	if s.ID() == 0 {
		t.Fatal("ID not assigned by Save")
	}
}

func TestServerRepo_ByID_RoundTrip(t *testing.T) {
	repo, _ := newServerRepo(t)
	s := mustAdd(t, repo, "main", 563, 0)

	got, err := repo.ByID(context.Background(), s.ID())
	if err != nil {
		t.Fatalf("ByID: %v", err)
	}
	if got.Name() != "main" || got.Host() != "host.main" || got.Port() != 563 {
		t.Errorf("round-tripped fields wrong: %+v", got)
	}
	if got.MaxConns() != 10 {
		t.Errorf("MaxConns = %d; want 10", got.MaxConns())
	}
}

func TestServerRepo_ByID_NotFound(t *testing.T) {
	repo, _ := newServerRepo(t)
	_, err := repo.ByID(context.Background(), 999)
	if !errors.Is(err, server.ErrNotFound) {
		t.Errorf("err = %v; want ErrNotFound", err)
	}
}

func TestServerRepo_ByName(t *testing.T) {
	repo, _ := newServerRepo(t)
	mustAdd(t, repo, "primary", 563, 0)
	got, err := repo.ByName(context.Background(), "primary")
	if err != nil {
		t.Fatalf("ByName: %v", err)
	}
	if got.Name() != "primary" {
		t.Errorf("name = %q", got.Name())
	}

	_, err = repo.ByName(context.Background(), "missing")
	if !errors.Is(err, server.ErrNotFound) {
		t.Errorf("err for missing name = %v; want ErrNotFound", err)
	}
}

func TestServerRepo_List_OrderedByPriority(t *testing.T) {
	repo, _ := newServerRepo(t)
	mustAdd(t, repo, "backup", 563, 5)
	mustAdd(t, repo, "primary", 563, 0)
	mustAdd(t, repo, "tertiary", 563, 10)

	all, err := repo.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("count = %d; want 3", len(all))
	}
	if all[0].Name() != "primary" || all[1].Name() != "backup" || all[2].Name() != "tertiary" {
		t.Errorf("order wrong: %s, %s, %s", all[0].Name(), all[1].Name(), all[2].Name())
	}
}

func TestServerRepo_ListEnabled_FiltersDisabled(t *testing.T) {
	repo, _ := newServerRepo(t)
	a := mustAdd(t, repo, "a", 563, 0)
	mustAdd(t, repo, "b", 563, 1)

	a.SetEnabled(false, time.Now())
	if err := repo.Save(context.Background(), a); err != nil {
		t.Fatalf("disable Save: %v", err)
	}

	enabled, err := repo.ListEnabled(context.Background())
	if err != nil {
		t.Fatalf("ListEnabled: %v", err)
	}
	if len(enabled) != 1 || enabled[0].Name() != "b" {
		t.Errorf("enabled = %v; want only [b]", names(enabled))
	}
}

func TestServerRepo_Update(t *testing.T) {
	repo, _ := newServerRepo(t)
	s := mustAdd(t, repo, "x", 1, 0)

	mc := 99
	if err := s.Update(server.UpdateParams{MaxConns: &mc}, time.Now()); err != nil {
		t.Fatalf("agg Update: %v", err)
	}
	if err := repo.Save(context.Background(), s); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := repo.ByID(context.Background(), s.ID())
	if err != nil {
		t.Fatalf("ByID: %v", err)
	}
	if got.MaxConns() != 99 {
		t.Errorf("MaxConns = %d; want 99", got.MaxConns())
	}
}

func TestServerRepo_Delete(t *testing.T) {
	repo, _ := newServerRepo(t)
	s := mustAdd(t, repo, "x", 1, 0)

	if err := repo.Delete(context.Background(), s.ID()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := repo.ByID(context.Background(), s.ID())
	if !errors.Is(err, server.ErrNotFound) {
		t.Errorf("post-delete ByID = %v; want ErrNotFound", err)
	}
	if err := repo.Delete(context.Background(), s.ID()); !errors.Is(err, server.ErrNotFound) {
		t.Errorf("redundant Delete = %v; want ErrNotFound", err)
	}
}

func TestServerRepo_NameUniqueness(t *testing.T) {
	repo, _ := newServerRepo(t)
	mustAdd(t, repo, "dup", 1, 0)

	s2, err := server.New(server.NewParams{Name: "dup", Host: "h2", Port: 2, MaxConns: 1}, time.Now())
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := repo.Save(context.Background(), s2); err == nil {
		t.Fatal("expected error on duplicate name")
	}
}

func names(ss []*server.UsenetServer) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = s.Name()
	}
	return out
}
