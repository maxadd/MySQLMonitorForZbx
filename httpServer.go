package main

import (
	"fmt"
	"github.com/golang/glog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func getUpTime() int64 {
	t := time.Now().Unix()
	return t - bootTime
}

func monitor(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.String(), "?")
	if len(s) != 2 {
		fmt.Fprintln(w, "Unsupported parameters")
		return
	}
	switch s[1] {
	case "uptime":
		fmt.Fprintln(w, getUpTime())
	default:
		fmt.Fprintln(w, "Unsupported parameters")
	}
}

func admin(w http.ResponseWriter, r *http.Request) {
	url := strings.Split(r.URL.String(), "?")
	if len(url) != 2 {
		fmt.Fprintln(w, "Unsupported parameters")
		return
	}

	value := strings.Split(url[1], "=")
	if len(value) != 2 {
		switch url[1] {
		case "flush":
			glog.Flush()
			fmt.Fprintln(w, "Log buffer manually refreshed successfully")
		default:
			fmt.Fprintln(w, "Unsupported parameters")
		}
		return
	}

	switch value[0] {
	case "verbose":
		i, err := strconv.Atoi(value[1])
		if err != nil {
			fmt.Fprintln(w, "Value must be a int")
		}
		glog.Logging.Verbosity = glog.Level(i)
		fmt.Fprintln(w, "ok")
	default:
		fmt.Fprintln(w, "Unsupported parameters")
	}
}

func startHttpServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/monitor", monitor)
	mux.HandleFunc("/admin", admin)
	err := http.ListenAndServe(":"+httpPort, mux)
	if err != nil {
		glog.Error("http listen failed")
		os.Exit(1)
	}
}
