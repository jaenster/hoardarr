package sqlite

import (
	"context"
	"errors"
	"testing"
)

func TestSettingsRepo_RoundTrip(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	r := NewSettingsRepo(db)

	// String round-trip.
	if err := r.Set(ctx, "k.str", "hello"); err != nil {
		t.Fatalf("set string: %v", err)
	}
	got, err := r.Get(ctx, "k.str")
	if err != nil || got != "hello" {
		t.Errorf("get string = %q, %v; want hello, nil", got, err)
	}

	// Int round-trip.
	if err := r.SetInt(ctx, "k.int", 42); err != nil {
		t.Fatalf("set int: %v", err)
	}
	n, err := r.GetIntOr(ctx, "k.int", 0)
	if err != nil || n != 42 {
		t.Errorf("get int = %d, %v; want 42, nil", n, err)
	}

	// Float round-trip.
	if err := r.SetFloat(ctx, "k.float", 0.075); err != nil {
		t.Fatalf("set float: %v", err)
	}
	f, err := r.GetFloatOr(ctx, "k.float", 0)
	if err != nil || f != 0.075 {
		t.Errorf("get float = %v, %v; want 0.075, nil", f, err)
	}

	// Bool round-trip (both states).
	for _, want := range []bool{true, false} {
		if err := r.SetBool(ctx, "k.bool", want); err != nil {
			t.Fatalf("set bool: %v", err)
		}
		got, err := r.GetBoolOr(ctx, "k.bool", !want)
		if err != nil || got != want {
			t.Errorf("get bool = %v, %v; want %v, nil", got, err, want)
		}
	}

	// Missing key returns ErrSettingNotFound on Get.
	if _, err := r.Get(ctx, "no.such.key"); !errors.Is(err, ErrSettingNotFound) {
		t.Errorf("Get missing = %v; want ErrSettingNotFound", err)
	}

	// Default returns on GetXxxOr.
	v, err := r.GetStringOr(ctx, "no.such.key", "fallback")
	if err != nil || v != "fallback" {
		t.Errorf("GetStringOr missing = %q, %v; want fallback, nil", v, err)
	}
}

func TestSettingsRepo_Overwrite(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	r := NewSettingsRepo(db)

	for _, v := range []string{"one", "two", "three"} {
		if err := r.Set(ctx, "k", v); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	got, _ := r.Get(ctx, "k")
	if got != "three" {
		t.Errorf("after overwrite got %q; want three", got)
	}
}
