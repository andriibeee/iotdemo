package config

import (
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	Server    Server    `koanf:"server"`
	Sink      Sink      `koanf:"sink"`
	Journal   Journal   `koanf:"journal"`
	Dedup     Dedup     `koanf:"dedup"`
	RateLimit RateLimit `koanf:"rate_limit"`
}

type Server struct {
	Addr         string        `koanf:"addr"`
	ReadTimeout  time.Duration `koanf:"read_timeout"`
	WriteTimeout time.Duration `koanf:"write_timeout"`
	TLS          TLS           `koanf:"tls"`
}

type TLS struct {
	Cert     string `koanf:"cert"`
	Key      string `koanf:"key"`
	ClientCA string `koanf:"client_ca"`
}

type Sink struct {
	BufferSize    int           `koanf:"buffer_size"`
	FlushInterval time.Duration `koanf:"flush_interval"`
}

type Journal struct {
	Dir           string `koanf:"dir"`
	MaxSize       int64  `koanf:"max_size"`
	EncryptionKey string `koanf:"encryption_key"`
}

type Dedup struct {
	Enabled          bool          `koanf:"enabled"`
	CleaningInterval time.Duration `koanf:"cleaning_interval"`
}

type RateLimit struct {
	Enabled     bool    `koanf:"enabled"`
	BytesPerSec float64 `koanf:"bytes_per_sec"`
}

func Load(path string) (*Config, error) {
	k := koanf.New(".")

	cfg := &Config{
		Server: Server{
			Addr:         ":8080",
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		Sink: Sink{
			BufferSize:    128,
			FlushInterval: time.Second,
		},
		Journal: Journal{
			Dir:     "./data/journal",
			MaxSize: 64 * 1024 * 1024,
		},
		Dedup: Dedup{
			Enabled:          true,
			CleaningInterval: 10 * time.Minute,
		},
		RateLimit: RateLimit{
			Enabled:     true,
			BytesPerSec: 1024 * 1024,
		},
	}

	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return nil, err
		}
	}

	if err := k.Load(env.Provider("", ".", func(s string) string {
		return strings.ToLower(strings.ReplaceAll(s, "__", "."))
	}), nil); err != nil {
		return nil, err
	}

	if err := k.Unmarshal("", cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
