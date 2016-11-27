package main

type config struct {
	Port        int
	StartAsREST bool
}

var defConfig = config{
	Port:        18018,
	StartAsREST: false,
}

func loadConfig() (*config, error) {

	return &defConfig, nil
}
