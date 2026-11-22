// Command fleetwire-api runs the tenant, operator and device API, the
// background job worker, and the CRL publisher. It also applies database
// migrations and mints the first operator token.
//
//	fleetwire-api [serve]                        run the API
//	fleetwire-api migrate                        apply migrations (owner credentials)
//	fleetwire-api create-operator-token -name N  print a new operator token once
//	fleetwire-api config-docs                    print the configuration reference
//	fleetwire-api healthcheck                    exit 0 when the local /readyz answers 200
//	fleetwire-api version
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crl"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/jobs"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/svc"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "fleetwire-api:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cmd := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	cfg := &apiConfig{cmd: cmd}
	fs := flag.NewFlagSet("fleetwire-api "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var tokenName string
	if cmd == "create-operator-token" {
		fs.StringVar(&tokenName, "name", "", "Name of the operator token (required).")
	}
	switch cmd {
	case "version":
		fmt.Fprintln(stdout, buildinfo.Get())
		return nil
	case "healthcheck":
		addr := os.Getenv("FLEETWIRE_API_ADMIN_LISTEN")
		if addr == "" {
			addr = "127.0.0.1:9090"
		}
		return svc.Healthcheck(ctx, addr)
	case "config-docs":
		md, err := config.Markdown(cfg)
		if err != nil {
			return err
		}
		fmt.Fprint(stdout, md)
		return nil
	case "serve", "migrate", "create-operator-token":
	default:
		return fmt.Errorf("unknown command %q (want serve, migrate, create-operator-token, config-docs, version)", cmd)
	}
	if err := config.Load(cfg, fs, args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "migrate":
		res, err := store.Migrate(ctx, cfg.OwnerDatabaseURL, store.MigrateOptions{RuntimeRole: cfg.RuntimeRole})
		if err != nil {
			return err
		}
		for _, r := range res {
			fmt.Fprintf(stdout, "applied %05d %s (%s)\n", r.Version, r.Source, r.Duration)
		}
		if len(res) == 0 {
			fmt.Fprintln(stdout, "database schema is up to date")
		}
		return nil
	case "create-operator-token":
		if tokenName == "" {
			return errors.New("-name is required")
		}
		id, secret, hash, err := tokens.NewKeyed(tokens.PrefixOperator)
		if err != nil {
			return err
		}
		if err := store.CreateOperatorToken(ctx, cfg.OwnerDatabaseURL, id, tokenName, hash); err != nil {
			return err
		}
		fmt.Fprintln(stderr, "operator token created; it is shown once and cannot be recovered:")
		fmt.Fprintln(stdout, secret.Reveal())
		return nil
	}
	return serve(ctx, cfg, stderr)
}

func serve(ctx context.Context, cfg *apiConfig, logOut io.Writer) error {
	log, err := telemetry.NewLogger(logOut, "fleetwire-api", cfg.LogLevel)
	if err != nil {
		return err
	}
	log.Info("starting", "build", buildinfo.Get().String())
	shutdownTracing, err := telemetry.InitTracing(ctx, "fleetwire-api", cfg.OTLPEndpoint, cfg.TraceSampleRatio)
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.WithoutCancel(ctx)) }()
	reg := telemetry.NewRegistry("fleetwire-api")

	db, err := store.Connect(ctx, cfg.DatabaseURL, store.Options{
		MaxConns: cfg.DBMaxConns, ApplicationName: "fleetwire-api", Tracer: otelpgx.NewTracer(),
	})
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.CheckRuntimeRole(ctx); err != nil {
		return err
	}

	rootKey, err := keyring.LoadRootKey(cfg.KeyringRootKeyFile)
	if err != nil {
		return err
	}
	kr, err := keyring.NewLocal(cfg.KeyringDir, rootKey)
	if err != nil {
		return err
	}
	cursorKey, err := keyring.DeriveKey(rootKey, "api-cursor")
	if err != nil {
		return err
	}
	ca, err := pki.LoadCA(cfg.DeviceCACertFile, cfg.DeviceCAKeyFile)
	if err != nil {
		return err
	}
	bundle, err := os.ReadFile(cfg.CABundleFile)
	if err != nil {
		return fmt.Errorf("read CA bundle: %w", err)
	}
	serverCert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return fmt.Errorf("load API TLS certificate: %w", err)
	}
	deviceCAs, err := pki.CertPool(cfg.DeviceTrustFile)
	if err != nil {
		return err
	}
	var internal *http.Server
	if cfg.InternalListen != "" {
		services, err := pki.CertPool(cfg.ServiceTrustFile)
		if err != nil {
			return err
		}
		internal = &http.Server{
			Addr:    cfg.InternalListen,
			Handler: keyring.NewServer(kr, log.With("listener", "internal"), "fleetwire-ingest"),
			TLSConfig: &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12,
				ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: services},
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       2 * time.Minute,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		}
	}
	keys := tenantkey.New(db, kr, log)
	pub := crl.NewPublisher(db, ca, cfg.CRLFile, log)
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "fleetwire_crl_age_seconds", Help: "Seconds since this process last published the CRL.",
	}, func() float64 { return pub.Age().Seconds() }))

	srv := api.New(api.Config{
		PublicAddr:        cfg.PublicListen,
		DeviceAddr:        cfg.DeviceListen,
		PublicTLS:         &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12},
		DeviceTLS:         &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: deviceCAs},
		CABundlePEM:       bundle,
		BrokerURL:         cfg.DeviceBrokerURL,
		DeviceAPIURL:      cfg.DeviceAPIURL,
		RatePerSecond:     cfg.RateLimit,
		RateBurst:         cfg.RateBurst,
		AuthFailPerSecond: cfg.AuthFailRateLimit,
		AuthFailBurst:     cfg.AuthFailRateBurst,
		EnrollPerSecond:   cfg.EnrollRateLimit,
		EnrollBurst:       cfg.EnrollRateBurst,
		EnrollTokenTTL:    cfg.EnrollTokenTTL,
		ExportDir:         cfg.ExportDir,
		MaxActiveExports:  cfg.MaxActiveExports,
	}, api.Deps{DB: db, Keyring: kr, Keys: keys, DeviceCA: ca, CRL: pub, CursorKey: cursorKey, Log: log, Registry: reg})

	worker := jobs.NewWorker(jobs.Config{
		Workers: cfg.JobWorkers, ExportDir: cfg.ExportDir, WorkerID: "api-" + uuid.NewString()[:8],
	}, jobs.Deps{DB: db, Keyring: kr, Keys: keys, CRL: pub, Log: log, Registry: reg})

	admin, err := telemetry.NewAdminServer(ctx, cfg.AdminListen, reg, map[string]telemetry.Probe{"database": db.Ping})
	if err != nil {
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(admin.Serve)
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 10*time.Second)
		defer cancel()
		return admin.Shutdown(sctx)
	})
	g.Go(func() error { pub.Run(gctx, cfg.CRLInterval); return nil })
	g.Go(func() error { return worker.Run(gctx) })
	g.Go(func() error { return srv.Serve(gctx) })
	if internal != nil {
		g.Go(func() error {
			log.Info("keyring listener started", "addr", cfg.InternalListen)
			if err := internal.ListenAndServeTLS("", ""); !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("keyring listener: %w", err)
			}
			return nil
		})
		g.Go(func() error {
			<-gctx.Done()
			sctx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 10*time.Second)
			defer cancel()
			return internal.Shutdown(sctx)
		})
	}
	err = g.Wait()
	log.Info("stopped", "err", err)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
