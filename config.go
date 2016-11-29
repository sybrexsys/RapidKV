package main

type config struct {
	Port        int
	StartAsREST bool
	AuthPass    string
	ShardCount  int
}

var defConfig = config{
	Port:        18018,
	StartAsREST: false,
	AuthPass:    "test",
	ShardCount:  32,
}

func loadConfig() (*config, error) {
	return &defConfig, nil
}
