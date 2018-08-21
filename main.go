package main

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net"
	"os"

	"context"
	"flag"
	"github.com/golang/glog"
	"strings"
	"time"
)

var (
	allSqlConfig map[string]hostAndSQL
	valueChan    chan *bodyAndIP
	bootTime     int64
	ctx          context.Context
)

func init() {
	bootTime = time.Now().Unix()
	ctx = context.Background()
}

func getConfigFile() string {
	var file string
	flag.StringVar(&file, "f", "", "config file")
	flag.Parse()
	if file == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -f CONFIG_NAME\n", os.Args[0])
		os.Exit(1)
	}
	return file
}

type hostAndSQL struct {
	ZbxAddr   string              `yaml:"zbx_addr"`
	Instances map[string]instance `yaml:"instances"`
}

type instance struct {
	User     string               `yaml:"user"`
	Password string               `yaml:"password"`
	Database string               `yaml:"database"`
	SQL      map[string]sqlConfig `yaml:"_sql"`
}

type sqlConfig struct {
	SQL       string `yaml:"sql"`
	Frequency int    `yaml:"frequency"`
	Flag      string `yaml:"flag"`
	Items     string `yaml:"items"`
}

type jsonContent struct {
	Request string    `json:"request"`
	Data    []zbxBody `json:"data"`
	Clock   int64     `json:"clock"`
}

type zbxBody struct {
	Host  string `json:"host"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Clock int64  `json:"clock"`
}

type bodyAndIP struct {
	body *zbxBody
	ip   string
}

type allAttr struct {
	mysqlIP   string
	mysqlPort string
	zbxAddr   string
	key       string
	sql       string
	frequency int
	conn      *sql.DB
	flag      string
	items     string
}

type lowLevelDiscovery struct {
	Data []map[string]string `json:"data"`
}

func Exit(code int) {
	glog.Flush()
	os.Exit(code)
}

func getSendData(sendData *jsonContent) []byte {
	bytes, e := json.Marshal(sendData)
	if e != nil {
		glog.Errorf("Marshal failed, %v\n", e)
		Exit(1)
	}

	dataLen := make([]byte, 8)
	binary.LittleEndian.PutUint32(dataLen, uint32(len(bytes)))
	data := append([]byte("ZBXD\x01"), dataLen...)
	data = append(data, bytes...)
	glog.V(1).Infof("the content sent to zabbix is %s", data)
	return data
}

func loadYaml(yamlFile string) {
	bytes, err := ioutil.ReadFile(yamlFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open yaml %s failed, %v\n", yamlFile, err)
		os.Exit(1)
	}
	err = yaml.Unmarshal(bytes, &allSqlConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse yaml %s failed, %v\n", yamlFile, err)
		os.Exit(1)
	}
	glog.Infof("sql config: %v", allSqlConfig)
}

func createZbxConn(zbxAddr string) net.Conn {
	conn, e := net.Dial("tcp", zbxAddr)
	if e != nil {
		fmt.Fprintf(os.Stderr, "connect server %s failed, %v\n", zbxAddr, e)
		os.Exit(1)
	}
	return conn
}

func createMysqlConn(ip, port, user, password, database string) *sql.DB {
	connStr := user + ":" + password + "@tcp(" + ip + ":" + port + ")/" + database
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		glog.Errorf("conn database failed, %v", err)
		Exit(1)
	}
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)
	return db
}

func genMetric(value, mysqlIP, key, zbxAddr string) {
	t := time.Now().Unix()
	a := &zbxBody{
		Host:  mysqlIP,
		Key:   key,
		Value: value,
		Clock: t,
	}
	glog.V(2).Infof("Metric: %v", a)
	valueChan <- &bodyAndIP{body: a, ip: zbxAddr}
}

func slaveStatus(p *allAttr) {
	subCtx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second*2))
	defer cancelFunc()
	rows, err := p.conn.QueryContext(subCtx, "SHOW SLAVE STATUS")
	defer rows.Close()
	if err != nil {
		glog.Errorf("Exec sql %s failed, %v", p.sql, err)
		Exit(1)
	}

	keys := strings.Split(p.key, "::")
	glog.V(3).Info("master/slave keys: %s", keys)
	if len(keys) == 0 {
		glog.Errorf("Error in key `%s -> %s -> _sql -> %s` in configuration file",
			p.mysqlIP, p.mysqlPort, p.key)
		Exit(1)
	}

	items := strings.Split(p.items, "::")
	glog.V(3).Info("master/slave items: %s", items)
	if len(items) == 0 {
		glog.Errorf("Error in items `%s -> %s -> _sql -> %s -> %s` in configuration file",
			p.mysqlIP, p.mysqlPort, p.key, p.items)
		Exit(1)
	}
	if len(keys) != len(items) {
		glog.Errorf("Items and keys Must correspond one by one in `%s -> %s -> _sql`",
			p.mysqlIP, p.mysqlPort)
		Exit(1)
	}

	cols, err := rows.Columns()
	if err != nil {
		glog.Errorf("Get columns,", err)
		Exit(1)
	}

	buff := make([]interface{}, len(cols))
	data := make([]string, len(cols))
	for i := range buff {
		buff[i] = &data[i]
	}
	for rows.Next() {
		rows.Scan(buff...)
	}

	// k 是索引，col 是值
	for k, col := range data {
		for n, v := range items {
			if cols[k] == v {
				genMetric(col, p.mysqlIP, keys[n], p.zbxAddr)
				break
			}
		}
	}
}

func getSQLValue(p *allAttr) {
	switch p.flag {
	case "m/s":
		slaveStatus(p)
	}
}

func execSQL(p *allAttr) {
	t := time.NewTicker(time.Duration(p.frequency) * time.Second)
	go func() {
		for {
			_ = <-t.C
			glog.V(1).Infof("start exec sql %s", p.key)
			getSQLValue(p)
		}
	}()
}

func getLLDMsg(data *lowLevelDiscovery, mysqlIP, key, zbxAddr string) {
	b, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		glog.Errorf("Get LLD json msg failed, %v", err)
		Exit(1)
	}
	genMetric(string(b), mysqlIP, key, zbxAddr)
}

func parseMap() {
	for mysqlIP, v := range allSqlConfig {
		discovery := lowLevelDiscovery{}
		for port, mysqlConf := range v.Instances {
			conn := createMysqlConn(mysqlIP, port, mysqlConf.User, mysqlConf.Password, mysqlConf.Database)
			for key, _sql := range mysqlConf.SQL {
				p := &allAttr{
					zbxAddr:   v.ZbxAddr,
					mysqlIP:   mysqlIP,
					key:       key,
					sql:       _sql.SQL,
					frequency: _sql.Frequency,
					conn:      conn,
					flag:      _sql.Flag,
					mysqlPort: port,
					items:     _sql.Items,
				}
				execSQL(p)
				time.Sleep(2 * time.Second)
			}
			discovery.Data = append(discovery.Data, map[string]string{"{#PORT}": port})
		}
		getLLDMsg(&discovery, mysqlIP, "db.instance", v.ZbxAddr)
	}
}

func sendMetricToZbx(ip string, arrayBody []zbxBody) {
	t := time.Now().Unix()
	a := &jsonContent{
		Request: "sender data",
		Data:    arrayBody,
		Clock:   t,
	}
	conn := createZbxConn(ip)
	_, err := conn.Write(getSendData(a))
	if err != nil {
		glog.Error("failed sending data to zabbix")
		Exit(1)
	}
	//resp := make([]byte, 1024)
	resp, err := ioutil.ReadAll(conn)
	if err != nil {
		glog.Errorf("Error reading response from zabibx, %v", err)
		Exit(1)
	}
	conn.Close()

	for n, b := range resp {
		if b == '{' {
			resp = resp[n:]
		}
	}
	glog.Info("zabbix response: %s", string(resp))
}

func buildMsgToSend(tmpMap map[string][]zbxBody) {
	for {
		select {
		case s := <-valueChan:
			_, ok := tmpMap[s.ip]
			if ok {
				//v = append(v, *s.body)
				tmpMap[s.ip] = append(tmpMap[s.ip], *s.body)
			} else {
				tmpMap[s.ip] = []zbxBody{}
				tmpMap[s.ip] = append(tmpMap[s.ip], *s.body)
			}
		default:
			for ip, bytes := range tmpMap {
				sendMetricToZbx(ip, bytes)
			}
			return
		}
	}
}

func WaitMetricToSend() {
	for {
		tmpMap := map[string][]zbxBody{}
		select {
		case s := <-valueChan:
			glog.V(1).Info("Get the data to be sent, send after 4 seconds")
			tmpMap[s.ip] = []zbxBody{}
			tmpMap[s.ip] = append(tmpMap[s.ip], *s.body)
			time.Sleep(4 * time.Second)
			buildMsgToSend(tmpMap)
		}

	}
}

func main() {
	loadYaml(getConfigFile())
	fmt.Println(allSqlConfig)
	go startHttpServer()
	valueChan = make(chan *bodyAndIP, 30)
	go WaitMetricToSend()
	parseMap()
	select {}
}
