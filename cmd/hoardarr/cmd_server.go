package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/jaenster/hoardarr"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// cmdServer dispatches `server add | list | rm`.
func cmdServer(args []string, logger *slog.Logger) error {
	if len(args) == 0 {
		return fmt.Errorf("server: subcommand required (add|list|rm)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return cmdServerAdd(rest, logger)
	case "list", "ls":
		return cmdServerList(rest, logger)
	case "rm", "remove", "delete":
		return cmdServerRemove(rest, logger)
	default:
		return fmt.Errorf("server: unknown subcommand %q", sub)
	}
}

func cmdServerAdd(args []string, logger *slog.Logger) error {
	fs := flag.NewFlagSet("server add", flag.ContinueOnError)
	configPath := fs.String("config", "./config.toml", "path to config.toml")
	name := fs.String("name", "", "display name (unique)")
	host := fs.String("host", "", "host name or IP")
	port := fs.Int("port", 563, "TCP port (563 for TLS-NNTP)")
	noTLS := fs.Bool("no-tls", false, "disable TLS (default is TLS on)")
	user := fs.String("user", "", "username (optional)")
	pass := fs.String("pass", "", "password (optional)")
	conns := fs.Int("conns", 8, "max concurrent connections")
	priority := fs.Int("priority", 0, "priority (lower = higher; 0 default)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *host == "" {
		return fmt.Errorf("server add: --name and --host are required")
	}

	app, err := loadApp(*configPath, logger)
	if err != nil {
		return err
	}
	defer func() { _ = app.Shutdown() }()

	tls := !*noTLS
	id, err := app.ServerService.Add(context.Background(), appserver.AddCmd{
		Name:     *name,
		Host:     *host,
		Port:     *port,
		TLS:      &tls,
		Username: *user,
		Password: *pass,
		MaxConns: *conns,
		Priority: *priority,
	})
	if err != nil {
		return err
	}
	logger.Info("server added", "id", id, "name", *name, "host", *host, "port", *port, "tls", tls)
	return nil
}

func cmdServerList(args []string, logger *slog.Logger) error {
	fs := flag.NewFlagSet("server list", flag.ContinueOnError)
	configPath := fs.String("config", "./config.toml", "path to config.toml")
	if err := fs.Parse(args); err != nil {
		return err
	}

	app, err := loadApp(*configPath, logger)
	if err != nil {
		return err
	}
	defer func() { _ = app.Shutdown() }()

	servers, err := app.ServerService.List(context.Background())
	if err != nil {
		return err
	}
	if len(servers) == 0 {
		logger.Info("no servers configured")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tNAME\tHOST:PORT\tTLS\tCONNS\tPRIORITY\tENABLED")
	for _, s := range servers {
		tls := "yes"
		if !s.TLS() {
			tls = "no"
		}
		enabled := "yes"
		if !s.Enabled() {
			enabled = "no"
		}
		fmt.Fprintf(tw, "%d\t%s\t%s:%d\t%s\t%d\t%d\t%s\n",
			s.ID(), s.Name(), s.Host(), s.Port(), tls, s.MaxConns(), s.Priority(), enabled)
	}
	return tw.Flush()
}

func cmdServerRemove(args []string, logger *slog.Logger) error {
	fs := flag.NewFlagSet("server rm", flag.ContinueOnError)
	configPath := fs.String("config", "./config.toml", "path to config.toml")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("server rm: <id> required")
	}
	id, err := strconv.ParseInt(rest[0], 10, 64)
	if err != nil {
		return fmt.Errorf("server rm: bad id %q", rest[0])
	}

	app, err := loadApp(*configPath, logger)
	if err != nil {
		return err
	}
	defer func() { _ = app.Shutdown() }()

	if err := app.ServerService.Remove(context.Background(), domainserver.ServerID(id)); err != nil {
		return err
	}
	logger.Info("server removed", "id", id)
	return nil
}

// loadApp is a small helper used by every subcommand. It loads config
// and constructs a bootstrap.App without starting the HTTP listener.
// Subcommands run synchronously and call Shutdown when done.
func loadApp(configPath string, logger *slog.Logger) (*bootstrap.App, error) {
	cfg, err := config.LoadOrCreate(configPath)
	if err != nil {
		return nil, err
	}
	return bootstrap.Build(context.Background(), cfg, hoardarr.FrontendFS, logger)
}
