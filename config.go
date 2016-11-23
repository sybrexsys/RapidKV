package main

type config struct {
}

var defConfig = config{}

func loadConfig() (*config, error) {
	return &config{}, nil
}
