package config

type Config struct {
	Listen  string
	DataDir string
}

func Default() Config {
	return Config{
		Listen:  ":8085",
		DataDir: "./data",
	}
}
