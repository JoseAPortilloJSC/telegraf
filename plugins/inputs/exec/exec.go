//go:generate ../../../tools/readme_config_includer/generator
package exec

import (
	"bufio"
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/models"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers/nagios"
	"github.com/kballard/go-shellquote"
)

//go:embed sample.conf
var sampleConfig string

var once sync.Once

const maxStderrBytes int = 512

type Exec struct {
	Commands    []string        `toml:"commands"`
	Command     interface{}     `toml:"command"` // legacy: string, now. []srting
	Environment []string        `toml:"environment"`
	IgnoreError bool            `toml:"ignore_error"`
	LogStdErr   bool            `toml:"log_stderr"`
	Timeout     config.Duration `toml:"timeout"`
	Log         telegraf.Logger `toml:"-"`

	parser telegraf.Parser

	runner runner

	// Allow post-processing of command exit codes
	exitCodeHandler   exitCodeHandlerFunc
	parseDespiteError bool
}

type exitCodeHandlerFunc func([]telegraf.Metric, error, []byte) []telegraf.Metric

type runner interface {
	run([]string) ([]byte, []byte, error)
}

type cmdSpec struct {
	args []string // pre-split, ready for exec.Command
	name string   // for error messages
}

type commandRunner struct {
	environment []string
	timeout     time.Duration
	debug       bool
}

func (*Exec) SampleConfig() string {
	return sampleConfig
}

func (e *Exec) Init() error {
	switch v := e.Command.(type) {
	case string:
		// Legacy single string command. Later processed by shellquote.Split
		//e.Log.Warn("Specifying 'command' as 'string' is deprecated since v1.38.0 and will be removed in v1.45.0. " +
		//	"Please convert your command into a list with each parameter being an own entry.")
		config.PrintOptionValueDeprecationNotice("inputs.exec", "command", false, telegraf.DeprecationInfo{
			Since:     "1.38.0",
			RemovalIn: "1.45.0",
			Notice:    "Use array syntax instead: [\"/bin/sh\", \"-c\", \"echo metric_value\"]",
		})
		if len(strings.TrimSpace(e.Command.(string))) == 0 {
			return errors.New("Command string cannot be empty")
		}
	case []interface{}:
		// New []string command. TOML might parse arrays as []interface{}
		for i, value := range v {
			if reflect.TypeOf(value).Kind() != reflect.String {
				return fmt.Errorf("Command array index `%d' contains non-string value in array: %v (type: %T)", i, value, value)
			}
			if len(strings.TrimSpace(value.(string))) == 0 {
				return fmt.Errorf("Command array index `%d' cannot be empty", i)
			}
		}
		if len(e.Command.([]string)) == 0 {
			return errors.New("Command array cannot be empty")
		}
		// if there was only one argument, and it contained spaces, warn the user
		// that they may have configured it wrong.
		if len(e.Command.([]string)) == 1 && strings.Contains(e.Command.([]string)[0], " ") {
			e.Log.Warn("The inputs.exec Command contained spaces but no arguments. " +
				"This setting expects the program and arguments as an array of strings, " +
				"not as a space-delimited string. See the plugin readme for an example.")
		}
	default:
		return fmt.Errorf("Command has invalid type %T, expected string or []string", e.Command)
	}

	e.runner = &commandRunner{
		environment: e.Environment,
		timeout:     time.Duration(e.Timeout),
		debug:       e.Log.Level().Includes(telegraf.Debug),
	}

	return nil
}

func (e *Exec) SetParser(parser telegraf.Parser) {
	e.parser = parser
	unwrapped, ok := parser.(*models.RunningParser)
	if ok {
		if _, ok := unwrapped.Parser.(*nagios.Parser); ok {
			e.exitCodeHandler = func(metrics []telegraf.Metric, err error, msg []byte) []telegraf.Metric {
				return nagios.AddState(err, msg, metrics)
			}
			e.parseDespiteError = true
		}
	}
}

func (e *Exec) Gather(acc telegraf.Accumulator) error {
	cmdSpecs := []cmdSpec{}

	// Shell-like string-based commands support
	for _, cmd := range e.updateRunners() {
		// Shell-like string-based command support
		splitCmd, err := shellquote.Split(cmd)
		if err != nil || len(splitCmd) == 0 {
			e.Log.Errorf("exec: unable to parse command %q: %w", cmd, err)
			continue
		}
		cmdSpecs = append(cmdSpecs, cmdSpec{
			args: splitCmd,
			name: cmd,
		})
	}
	// Array-based single command support
	if splitCmd, ok := e.Command.([]string); ok {
		cmdSpecs = append(cmdSpecs, cmdSpec{
			args: splitCmd,
			name: strings.Join(splitCmd, " "),
		})
	}

	var wg sync.WaitGroup
	for _, cmdSpec := range cmdSpecs {
		wg.Add(1)

		go func(c cmdSpec) {
			defer wg.Done()
			acc.AddError(e.processCommand(acc, c))
		}(cmdSpec)
	}
	wg.Wait()
	return nil
}

func (e *Exec) updateRunners() []string {
	commands := make([]string, 0, len(e.Commands))
	for _, pattern := range e.Commands {
		if pattern == "" {
			continue
		}

		// Try to expand globbing expressions
		cmd, args, found := strings.Cut(pattern, " ")
		matches, err := filepath.Glob(cmd)
		if err != nil {
			e.Log.Errorf("Matching command %q failed: %v", cmd, err)
			continue
		}

		if len(matches) == 0 {
			// There were no matches with the glob pattern, so let's assume
			// the command is in PATH and just run it as it is
			commands = append(commands, pattern)
		} else {
			// There were matches, so we'll append each match together with
			// the arguments to the commands slice
			for _, match := range matches {
				if found {
					match += " " + args
				}
				commands = append(commands, match)
			}
		}
	}

	return commands
}

func (e *Exec) processCommand(acc telegraf.Accumulator, cmdspec cmdSpec) error {
	out, errBuf, runErr := e.runner.run(cmdspec.args)
	if !e.IgnoreError && !e.parseDespiteError && runErr != nil {
		return fmt.Errorf("exec: %w for command %q: %s", runErr, cmdspec.name, string(errBuf))
	}

	// Log output in stderr
	if e.LogStdErr && len(errBuf) > 0 {
		scanner := bufio.NewScanner(bytes.NewBuffer(errBuf))

		for scanner.Scan() {
			msg := scanner.Text()
			switch {
			case strings.TrimSpace(msg) == "":
				continue
			case strings.HasPrefix(msg, "E! "):
				e.Log.Error(msg[3:])
			case strings.HasPrefix(msg, "W! "):
				e.Log.Warn(msg[3:])
			case strings.HasPrefix(msg, "I! "):
				e.Log.Info(msg[3:])
			case strings.HasPrefix(msg, "D! "):
				e.Log.Debug(msg[3:])
			case strings.HasPrefix(msg, "T! "):
				e.Log.Trace(msg[3:])
			default:
				e.Log.Error(msg)
			}
		}

		if err := scanner.Err(); err != nil {
			acc.AddError(fmt.Errorf("error reading stderr: %w", err))
		}
	}

	metrics, err := e.parser.Parse(out)
	if err != nil {
		return err
	}

	if len(metrics) == 0 {
		once.Do(func() {
			e.Log.Debug(internal.NoMetricsCreatedMsg)
		})
	}

	if e.exitCodeHandler != nil {
		metrics = e.exitCodeHandler(metrics, runErr, errBuf)
	}

	for _, m := range metrics {
		acc.AddMetric(m)
	}

	return nil
}

func truncate(buf *bytes.Buffer) {
	// Limit the number of bytes.
	didTruncate := false
	if buf.Len() > maxStderrBytes {
		buf.Truncate(maxStderrBytes)
		didTruncate = true
	}
	if i := bytes.IndexByte(buf.Bytes(), '\n'); i > 0 {
		// Only show truncation if the newline wasn't the last character.
		if i < buf.Len()-1 {
			didTruncate = true
		}
		buf.Truncate(i)
	}
	if didTruncate {
		buf.WriteString("...")
	}
}

func init() {
	inputs.Add("exec", func() telegraf.Input {
		return &Exec{
			Timeout: config.Duration(5 * time.Second),
		}
	})
}
