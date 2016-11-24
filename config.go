package main

type config struct {
	Port        int
	StartAsREST bool
}

var defConfig = config{}

func loadConfig() (*config, error) {
	return &config{}, nil
}
