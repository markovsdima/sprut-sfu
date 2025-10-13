package config

import (
	"log"
	"os"

	"github.com/goccy/go-yaml"
	"github.com/pion/webrtc/v4"
)

type Config struct {
	Server struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		LogLevel string `yaml:"log_level"`
	} `yaml:"server"`

	WebRTC struct {
		ICEServers []struct {
			URLs       []string `yaml:"urls"`
			Username   string   `yaml:"username,omitempty"`
			Credential string   `yaml:"credential,omitempty"`
		} `yaml:"ice_servers"`
	} `yaml:"webrtc"`

	ICE struct {
		UDPPortRange struct {
			Min int `yaml:"min"`
			Max int `yaml:"max"`
		} `yaml:"udp_port_range"`
	} `yaml:"ice"`

	Rooms struct {
		MaxParticipants int    `yaml:"max_participants"`
		IdleTimeout     string `yaml:"idle_timeout"`
		CleanupInterval string `yaml:"cleanup_interval"`
	} `yaml:"rooms"`

	Environment string `yaml:"environment"`
}

// LoadConfig reads YAML file from disk
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Ensure ICEServers slice is initialized
	if cfg.WebRTC.ICEServers == nil {
		cfg.WebRTC.ICEServers = make([]struct {
			URLs       []string `yaml:"urls"`
			Username   string   `yaml:"username,omitempty"`
			Credential string   `yaml:"credential,omitempty"`
		}, 0)
	}

	log.Printf("📋 Loaded config: ICE servers count = %d", len(cfg.WebRTC.ICEServers))

	return &cfg, nil
}

// MustLoad terminates the app if config loading fails
func MustLoad() *Config {
	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ failed to load config.yaml: %v", err)
	}
	return cfg
}

// GetWebRTCConfig converts config to pion/webrtc format
func (c *Config) GetWebRTCConfig() webrtc.Configuration {
	if c == nil {
		log.Printf("⚠️  Config is nil, using defaults")
		return getDefaultWebRTCConfig()
	}

	log.Printf("🔍 GetWebRTCConfig called with config having %d ICE servers", len(c.WebRTC.ICEServers))

	if len(c.WebRTC.ICEServers) == 0 {
		log.Printf("⚠️  Warning: No ICE servers configured, using defaults")
		return getDefaultWebRTCConfig()
	}

	log.Printf("📡 Using %d ICE servers from config", len(c.WebRTC.ICEServers))

	iceServers := make([]webrtc.ICEServer, len(c.WebRTC.ICEServers))

	for i, server := range c.WebRTC.ICEServers {
		iceServers[i] = webrtc.ICEServer{
			URLs:       server.URLs,
			Username:   server.Username,
			Credential: server.Credential,
		}
	}

	return webrtc.Configuration{
		ICEServers: iceServers,
	}
}

func getDefaultWebRTCConfig() webrtc.Configuration {
	return webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	}
}
