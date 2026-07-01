package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/version"
)

// TestFlagValueFromArgs covers the shared value-flag parser, including the
// malformed trailing-flag case where no value follows.
func TestFlagValueFromArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		flag string
		want string
	}{
		{"absent", []string{"nram"}, "--workdir", ""},
		{"present", []string{"nram", "--workdir", "/srv/nram"}, "--workdir", "/srv/nram"},
		{"with-other-flags", []string{"nram", "--config", "c.yaml", "--workdir", "/data"}, "--workdir", "/data"},
		{"trailing-without-value", []string{"nram", "--workdir"}, "--workdir", ""},
		{"config-flag", []string{"nram", "--config", "c.yaml"}, "--config", "c.yaml"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := flagValueFromArgs(tc.args, tc.flag); got != tc.want {
				t.Errorf("flagValueFromArgs(%v, %q) = %q, want %q", tc.args, tc.flag, got, tc.want)
			}
		})
	}
}

// TestApplyWorkdirChangesDirectory verifies applyWorkdir chdirs when --workdir
// is set and is a no-op otherwise. It restores the original directory so it does
// not disturb sibling tests.
func TestApplyWorkdirChangesDirectory(t *testing.T) {
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	target := t.TempDir()
	if err := applyWorkdir([]string{"nram", "--workdir", target}); err != nil {
		t.Fatalf("applyWorkdir: %v", err)
	}
	got, _ := os.Getwd()
	// macOS /var is a symlink to /private/var, so compare resolved paths.
	gotResolved, _ := filepath.EvalSymlinks(got)
	wantResolved, _ := filepath.EvalSymlinks(target)
	if gotResolved != wantResolved {
		t.Errorf("cwd after applyWorkdir = %q, want %q", gotResolved, wantResolved)
	}

	// No --workdir: cwd must not change.
	if err := applyWorkdir([]string{"nram"}); err != nil {
		t.Fatalf("applyWorkdir (no flag): %v", err)
	}
	after, _ := os.Getwd()
	if after != got {
		t.Errorf("applyWorkdir with no flag changed cwd from %q to %q", got, after)
	}
}

// TestServiceConfigIdentityAndArgs asserts the installed service records the
// product identity, captures an absolute working directory, and threads a
// supplied --config through as an absolute path.
func TestServiceConfigIdentityAndArgs(t *testing.T) {
	cfg, err := serviceConfig([]string{"nram", "service", "install", "--config", "config.yaml"})
	if err != nil {
		t.Fatalf("serviceConfig: %v", err)
	}

	if cfg.Name != version.Short {
		t.Errorf("Name = %q, want %q", cfg.Name, version.Short)
	}
	if cfg.DisplayName != version.Name {
		t.Errorf("DisplayName = %q, want %q", cfg.DisplayName, version.Name)
	}
	if !filepath.IsAbs(cfg.WorkingDirectory) {
		t.Errorf("WorkingDirectory = %q, want an absolute path", cfg.WorkingDirectory)
	}

	// Arguments: --workdir <abs cwd> --config <abs config>.
	if len(cfg.Arguments) != 4 || cfg.Arguments[0] != "--workdir" || cfg.Arguments[2] != "--config" {
		t.Fatalf("Arguments = %v, want [--workdir <abs> --config <abs>]", cfg.Arguments)
	}
	if !filepath.IsAbs(cfg.Arguments[1]) {
		t.Errorf("--workdir value %q is not absolute", cfg.Arguments[1])
	}
	if !filepath.IsAbs(cfg.Arguments[3]) {
		t.Errorf("--config value %q is not absolute", cfg.Arguments[3])
	}
}

// TestServiceConfigOmitsConfigWhenAbsent asserts that without --config the run
// arguments carry only --workdir.
func TestServiceConfigOmitsConfigWhenAbsent(t *testing.T) {
	cfg, err := serviceConfig([]string{"nram", "service", "install"})
	if err != nil {
		t.Fatalf("serviceConfig: %v", err)
	}
	if len(cfg.Arguments) != 2 || cfg.Arguments[0] != "--workdir" {
		t.Errorf("Arguments = %v, want just [--workdir <abs>]", cfg.Arguments)
	}
}

// TestServiceConfigRestartOnFailureOptions asserts the per-platform
// restart-on-failure knobs are set: systemd Restart=on-failure, Windows
// OnFailure=restart, and a custom launchd template requesting restart only on a
// non-zero exit (KeepAlive {SuccessfulExit: false}).
func TestServiceConfigRestartOnFailureOptions(t *testing.T) {
	cfg, err := serviceConfig([]string{"nram", "service", "install"})
	if err != nil {
		t.Fatalf("serviceConfig: %v", err)
	}

	if got := cfg.Option["Restart"]; got != "on-failure" {
		t.Errorf(`Option["Restart"] = %v, want "on-failure"`, got)
	}
	if got := cfg.Option["OnFailure"]; got != "restart" {
		t.Errorf(`Option["OnFailure"] = %v, want "restart"`, got)
	}
	if got := cfg.Option["RunAtLoad"]; got != true {
		t.Errorf(`Option["RunAtLoad"] = %v, want true (start at boot)`, got)
	}

	launchd, ok := cfg.Option["LaunchdConfig"].(string)
	if !ok {
		t.Fatalf(`Option["LaunchdConfig"] is not a string: %T`, cfg.Option["LaunchdConfig"])
	}
	if !strings.Contains(launchd, "SuccessfulExit") {
		t.Errorf("launchd template missing SuccessfulExit key:\n%s", launchd)
	}
	// The KeepAlive dict must key restart on failure, i.e. SuccessfulExit false.
	idx := strings.Index(launchd, "SuccessfulExit")
	if idx < 0 || !strings.Contains(launchd[idx:], "<false/>") {
		t.Errorf("launchd KeepAlive should be a dict with SuccessfulExit <false/>:\n%s", launchd)
	}

	// Without --user the service must be system-level (no UserService option).
	if _, present := cfg.Option["UserService"]; present {
		t.Error(`Option["UserService"] should be unset without --user`)
	}
}

// TestServiceConfigUserFlag asserts --user requests a per-user service.
func TestServiceConfigUserFlag(t *testing.T) {
	cfg, err := serviceConfig([]string{"nram", "service", "install", "--user"})
	if err != nil {
		t.Fatalf("serviceConfig: %v", err)
	}
	if got := cfg.Option["UserService"]; got != true {
		t.Errorf(`Option["UserService"] = %v, want true`, got)
	}
}

// TestServiceConfigCapturesOperationalEnv asserts install snapshots the
// operational config env vars set in the shell and never captures the admin
// bootstrap secrets.
func TestServiceConfigCapturesOperationalEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/nram")
	t.Setenv("PORT", "9999")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("NRAM_ADMIN_PASS", "s3cret")

	cfg, err := serviceConfig([]string{"nram", "service", "install"})
	if err != nil {
		t.Fatalf("serviceConfig: %v", err)
	}

	if cfg.EnvVars["DATABASE_URL"] != "postgres://localhost/nram" {
		t.Errorf("DATABASE_URL = %q, want it captured", cfg.EnvVars["DATABASE_URL"])
	}
	if cfg.EnvVars["PORT"] != "9999" {
		t.Errorf("PORT = %q, want %q", cfg.EnvVars["PORT"], "9999")
	}
	if cfg.EnvVars["LOG_LEVEL"] != "debug" {
		t.Errorf("LOG_LEVEL = %q, want %q", cfg.EnvVars["LOG_LEVEL"], "debug")
	}
	if _, present := cfg.EnvVars["NRAM_ADMIN_PASS"]; present {
		t.Error("NRAM_ADMIN_PASS must never be captured into the service")
	}
}

// TestServiceConfigSkipsUnsetEnv asserts unset operational vars are not written
// as empty entries.
func TestServiceConfigSkipsUnsetEnv(t *testing.T) {
	// Empty operational vars must be treated as unset (mirrors config.applyEnv).
	for _, k := range operationalEnvVars {
		t.Setenv(k, "")
	}

	cfg, err := serviceConfig([]string{"nram", "service", "install"})
	if err != nil {
		t.Fatalf("serviceConfig: %v", err)
	}
	if len(cfg.EnvVars) != 0 {
		t.Errorf("EnvVars = %v, want empty when no operational vars are set", cfg.EnvVars)
	}
}

// TestDispatchServiceCommandRejectsBadInput asserts the parse-error paths
// (missing action, help token, unknown action) return the focused usage without
// touching the OS service manager.
func TestDispatchServiceCommandRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"missing-action", []string{"nram", "service"}, "usage: nram service"},
		{"help-token", []string{"nram", "service", "--help"}, "usage: nram service"},
		{"unknown-action", []string{"nram", "service", "bogus"}, "unknown service command"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := dispatchServiceCommand(tc.args)
			if err == nil {
				t.Fatalf("dispatchServiceCommand(%v) = nil, want error", tc.args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}

// TestServiceHelpIsHandled asserts the top-level info dispatcher answers
// `nram service --help` (returns true, meaning handled-and-exit).
func TestServiceHelpIsHandled(t *testing.T) {
	if !handleInfoFlags([]string{"nram", "service", "--help"}) {
		t.Error("handleInfoFlags did not handle `service --help`")
	}
}
