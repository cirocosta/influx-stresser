package main

import (
	"fmt"
	"time"

	"github.com/alexflint/go-arg"

	influx "github.com/influxdata/influxdb/client/v2"
	_ "github.com/rs/zerolog"
)

type cliConfig struct {
	Database string `arg:"env,required"`
	Host     string `arg:"env"`
	Port     int    `arg:"env"`
	User     string `arg:"env"`
	Password string `arg:"env"`
}

var (
	cli = &cliConfig{
		Database: "",
		Host:     "localhost",
		Port:     8086,
		User:     "",
		Password: "",
	}
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	arg.MustParse(cli)
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     fmt.Sprintf("http://%s:%d", cli.Host, cli.Port),
		Username: cli.User,
		Password: cli.Password,
	})
	must(err)

	// Create a new point batch
	bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  cli.Database,
		Precision: "s",
	})
	must(err)

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}

	pt, err := influx.NewPoint("cpu_usage", tags, fields, time.Now())
	must(err)

	bp.AddPoint(pt)

	// Write the batch
	err = c.Write(bp)
	must(err)
}
