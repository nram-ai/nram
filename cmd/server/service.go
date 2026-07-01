package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/kardianos/service"

	"github.com/nram-ai/nram/internal/version"
)

// operationalEnvVars are the nram configuration variables that `service install`
// snapshots from the installing shell into the service, so the installed service
// runs against the same database, port, and log level. The first-boot admin
// bootstrap secrets (NRAM_ADMIN_EMAIL/NRAM_ADMIN_PASS) are deliberately excluded
// so a plaintext password is never written into a unit/plist that is
// world-readable on Linux and macOS. The set mirrors internal/config applyEnv.
var operationalEnvVars = []string{"DATABASE_URL", "PORT", "LOG_LEVEL", "NRAM_CONFIG"}

// serviceDescription is the long description registered with the OS service
// manager (systemd Description=, launchd label metadata, Windows SCM
// description).
const serviceDescription = version.Name + " (" + version.Short + "): " + version.Tagline

// launchdOnFailureConfig is a custom launchd plist template handed to kardianos
// via Option["LaunchdConfig"]. It is the library's default template with one
// change: KeepAlive is emitted as a dict {SuccessfulExit: false} instead of a
// bare boolean, which makes launchd restart the job only when it exits with a
// non-zero status (restart-on-failure), matching the systemd Restart=on-failure
// and Windows SetRecoveryActions(restart) behaviour on the other platforms.
// kardianos parses this with its own FuncMap (the bool and html funcs).
const launchdOnFailureConfig = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Disabled</key>
	<false/>
	{{- if .EnvVars}}
	<key>EnvironmentVariables</key>
	<dict>
		{{- range $k, $v := .EnvVars}}
		<key>{{html $k}}</key>
		<string>{{html $v}}</string>
		{{- end}}
	</dict>
	{{- end}}
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>Label</key>
	<string>{{html .Name}}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{{html .Path}}</string>
		{{- if .Config.Arguments}}
		{{- range .Config.Arguments}}
		<string>{{html .}}</string>
		{{- end}}
	{{- end}}
	</array>
	{{- if .ChRoot}}
	<key>RootDirectory</key>
	<string>{{html .ChRoot}}</string>
	{{- end}}
	<key>RunAtLoad</key>
	<{{bool .RunAtLoad}}/>
	<key>SessionCreate</key>
	<{{bool .SessionCreate}}/>
	{{- if .StandardErrorPath}}
	<key>StandardErrorPath</key>
	<string>{{html .StandardErrorPath}}</string>
	{{- end}}
	{{- if .StandardOutPath}}
	<key>StandardOutPath</key>
	<string>{{html .StandardOutPath}}</string>
	{{- end}}
	{{- if .UserName}}
	<key>UserName</key>
	<string>{{html .UserName}}</string>
	{{- end}}
	{{- if .WorkingDirectory}}
	<key>WorkingDirectory</key>
	<string>{{html .WorkingDirectory}}</string>
	{{- end}}
</dict>
</plist>
`

// program adapts the nram HTTP server to the kardianos service.Interface. The
// server and its shutdown path are built in main (the ~1450-line app wiring is
// left intact); program only starts and stops the already-constructed
// http.Server. Start must return promptly, so ListenAndServe runs in a
// goroutine; Stop performs the same graceful shutdown the old signal handler
// did. The deferred context cancels and Close calls in main fire when main
// returns after svc.Run() unblocks, preserving the original teardown order.
type program struct {
	srv      *http.Server
	addr     string
	logLevel string
}

// Start satisfies service.Interface. It launches the HTTP listener in the
// background and returns immediately so the service manager sees a timely
// start.
func (p *program) Start(service.Service) error {
	go p.serve()
	return nil
}

// serve runs the blocking HTTP listener. A listen failure is fatal (matching
// the previous inline behaviour); the service manager observes the non-zero
// exit and, with restart-on-failure configured, restarts the service.
func (p *program) serve() {
	slog.Info("boot: server starting", "addr", p.addr, "log_level", p.logLevel)
	if err := p.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server failed to start", "err", err)
		os.Exit(1)
	}
}

// Stop satisfies service.Interface, gracefully draining in-flight requests with
// the same 10s deadline the old SIGINT/SIGTERM path used.
func (p *program) Stop(service.Service) error {
	slog.Info("boot: server shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.srv.Shutdown(ctx); err != nil {
		slog.Error("server forced to shutdown", "err", err)
		return err
	}
	slog.Info("boot: server stopped")
	return nil
}

// serviceConfig builds the kardianos service.Config shared by the run path
// (svc.Run) and the control path (install/uninstall/...). The restart-on-failure
// options are set for all three managers; they are only consumed at install
// time, so setting them unconditionally is harmless for other actions.
//
// The installed service is registered to run `nram --workdir <abs cwd>
// [--config <abs path>]`, capturing the directory and config in effect at
// install time. --workdir makes the server chdir before any CWD-relative read
// (config.yaml, nram.db) regardless of the launch directory the manager
// chooses; this also covers Windows, where kardianos does not honour
// Config.WorkingDirectory.
func serviceConfig(args []string) (*service.Config, error) {
	// os.Getwd already returns an absolute path.
	absCwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("resolve working directory: %w", err)
	}

	runArgs := []string{"--workdir", absCwd}
	if cfgPath := configPathFromArgs(args); cfgPath != "" {
		absCfg, err := filepath.Abs(cfgPath)
		if err != nil {
			return nil, fmt.Errorf("resolve config path: %w", err)
		}
		runArgs = append(runArgs, "--config", absCfg)
	}

	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve executable path: %w", err)
	}
	if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
		exe = resolved
	}

	options := service.KeyValue{
		// Start the service when the manager loads it (at boot/login and on
		// `service start`). systemd enables the unit and Windows uses automatic
		// start already; launchd defaults RunAtLoad to false, so set it here so
		// the (custom) launchd template starts at load and restart-on-failure is
		// meaningful across reboots.
		"RunAtLoad": true,
		// Linux (systemd): restart the unit only on non-zero exit.
		"Restart": "on-failure",
		// macOS (launchd): kardianos exposes only a boolean KeepAlive, so a
		// custom template supplies KeepAlive {SuccessfulExit: false}.
		"LaunchdConfig": launchdOnFailureConfig,
		// Windows (SCM): install a restart recovery action. The option keys and
		// "restart" value are Windows-only constants in kardianos (build-tagged),
		// so they are written as string literals here; the systemd and launchd
		// backends ignore unknown options.
		"OnFailure":              "restart",
		"OnFailureDelayDuration": "5s",
		"OnFailureResetPeriod":   86400,
	}
	if slices.Contains(args, "--user") {
		options["UserService"] = true
	}

	// Snapshot the operational config env vars set in the installing shell so the
	// service runs with the same database/port/logging. Admin bootstrap secrets
	// are intentionally not captured (see operationalEnvVars).
	env := make(map[string]string)
	for _, k := range operationalEnvVars {
		// Mirror internal/config applyEnv, which applies these vars only when
		// non-empty; capturing an empty value would write a useless
		// Environment=KEY= line into the unit.
		if v := os.Getenv(k); v != "" {
			env[k] = v
		}
	}

	return &service.Config{
		Name:             version.Short,
		DisplayName:      version.Name,
		Description:      serviceDescription,
		Executable:       exe,
		Arguments:        runArgs,
		WorkingDirectory: absCwd,
		Option:           options,
		EnvVars:          env,
	}, nil
}

// buildService constructs the kardianos Service for prg using the shared config
// derived from args.
func buildService(prg *program, args []string) (service.Service, error) {
	cfg, err := serviceConfig(args)
	if err != nil {
		return nil, err
	}
	return service.New(prg, cfg)
}

// dispatchServiceCommand handles `nram service <action> [--user] [--config
// <path>]`. It runs one control action against the OS service manager and
// exits; it never starts the HTTP server (Start/Stop are only invoked by
// svc.Run on the default path). Control actions come from kardianos'
// service.ControlAction (install/uninstall/start/stop/restart); status is
// separate because it is not a Control action.
func dispatchServiceCommand(args []string) error {
	if len(args) < 3 || hasHelpToken(args[2:]) {
		return fmt.Errorf("%s", serviceUsage)
	}
	action := args[2]

	isControl := slices.Contains(service.ControlAction[:], action)
	if !isControl && action != "status" {
		return fmt.Errorf("unknown service command %q\n\n%s", action, serviceUsage)
	}

	svc, err := buildService(&program{}, args)
	if err != nil {
		return err
	}

	if action == "status" {
		return printServiceStatus(svc)
	}
	if err := service.Control(svc, action); err != nil {
		return fmt.Errorf("service %s: %w", action, err)
	}
	fmt.Printf("%s service %s: ok\n", version.Short, action)
	return nil
}

// printServiceStatus reports the manager's view of the service.
func printServiceStatus(svc service.Service) error {
	st, err := svc.Status()
	if err != nil {
		return fmt.Errorf("service status: %w", err)
	}
	switch st {
	case service.StatusRunning:
		fmt.Printf("%s service: running\n", version.Short)
	case service.StatusStopped:
		fmt.Printf("%s service: stopped\n", version.Short)
	default:
		fmt.Printf("%s service: unknown (not installed?)\n", version.Short)
	}
	return nil
}

// applyWorkdir changes the process working directory when --workdir is present,
// before any CWD-relative read (config load, ./nram.db). It is called first in
// main so the installed service resolves its files from the directory captured
// at install time, independent of the directory the service manager launches
// from.
func applyWorkdir(args []string) error {
	dir := flagValueFromArgs(args, "--workdir")
	if dir == "" {
		return nil
	}
	if err := os.Chdir(dir); err != nil {
		return fmt.Errorf("apply --workdir %q: %w", dir, err)
	}
	return nil
}
