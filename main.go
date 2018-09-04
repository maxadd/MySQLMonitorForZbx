package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net"
	"os"

	"flag"
	"github.com/golang/glog"
	"strings"
	"time"
)

var (
	allSqlConfig  map[string]hostAndSQL
	valueChan     chan *bodyAndIP
	bootTime      int64
	httpPort      string
	maxMysqlPool  int
	idleMysqlPool int
	msChan        chan *bodyAndIP
	notifyChan    chan bool
	sqlChan       chan allAttr
)

func init() {
	bootTime = time.Now().Unix()
	notifyChan = make(chan bool, 1)
	sqlChan = make(chan allAttr, 10000)
	valueChan = make(chan *bodyAndIP, 50)
	msChan = make(chan *bodyAndIP, 2)
	go startHttpServer()
	go getPendingExecSql()
	go waitMetricToSend()
	go getLLDMsgFromChan()
}

func getConfigFile() string {
	var file string
	flag.IntVar(&maxMysqlPool, "maxpool", 3, "mysql connection pool size")
	flag.IntVar(&idleMysqlPool, "idlepool", 3, "mysql connection pool maximum idle connection, it should be less than or equal to maxpool")
	flag.StringVar(&file, "f", "", "config file")
	flag.StringVar(&httpPort, "p", "1025", "program-initiated http port for providing monitoring data and interacting with the program")
	//flag.StringVar(&lldKey, "k", "", "zabbix low-level discovery key for mysql multi-instance monitoring")
	flag.Parse()

	var t []string
	if file == "" {
		t = append(t, "-f")
	}

	if len(t) > 0 {
		fmt.Fprintf(os.Stderr, "Missing required options: %s\n", strings.Join(t, ", "))
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
	PtKey     string `yaml:"pt_key"`
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
	ptKey     string
}

type lowLevelDiscovery struct {
	Data []map[string]string `json:"data"`
}

func Exit(code int) {
	glog.Flush()
	os.Exit(code)
}

func getSendData(sendData *jsonContent) []byte {
	b, e := json.Marshal(sendData)
	if e != nil {
		glog.Errorf("Marshal failed, %v\n", e)
		Exit(1)
	}

	dataLen := make([]byte, 8)
	binary.LittleEndian.PutUint32(dataLen, uint32(len(b)))
	data := append([]byte("ZBXD\x01"), dataLen...)
	data = append(data, b...)
	glog.V(1).Infof("the content sent to zabbix is %s", data)
	return data
}

func loadYaml(yamlFile string) {
	b, err := ioutil.ReadFile(yamlFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open yaml %s failed, %v\n", yamlFile, err)
		os.Exit(1)
	}
	err = yaml.Unmarshal(b, &allSqlConfig)
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

func (p *allAttr) normalQuery(rows *sql.Rows) {
	var value string
	for rows.Next() {
		rows.Scan(&value)
	}
	glog.V(2).Infof("exec %s get value: %s", p.sql, value)
	a := genMetric(value, p.mysqlIP, p.key)
	valueChan <- &bodyAndIP{body: a, ip: p.zbxAddr}

}

func (p *allAttr) slaveStatus(rows *sql.Rows) {
	if p.ptKey == "" {
		glog.Errorf("Missing configuration items under %s/%s/%s of configuration file",
			p.mysqlIP, p.mysqlPort, p.key)
		Exit(1)
	}
	//keys := strings.Split(p.key, "::")
	//glog.V(3).Info("master/slave keys: %s", keys)
	//if len(keys) == 0 {
	//	glog.Errorf("Error in key `%s -> %s -> _sql -> %s` in configuration file",
	//		p.mysqlIP, p.mysqlPort, p.key)
	//	Exit(1)
	//}

	items := strings.Split(p.items, "::")
	glog.V(3).Info("master/slave items: %s", items)
	//if len(keys) == 0 {
	//	glog.Errorf("Error in items `%s -> %s -> _sql -> %s` in configuration file",
	//		p.mysqlIP, p.mysqlPort, p.key)
	//	Exit(1)
	//}
	//if len(keys) != len(items) {
	//	glog.Errorf("Items and keys Must correspond one by one in `%s -> %s -> _sql`",
	//		p.mysqlIP, p.mysqlPort)
	//	Exit(1)
	//}

	// cols 里面都是字段名
	cols, err := rows.Columns()
	if err != nil {
		glog.Errorf("Get columns failed,", err)
		Exit(1)
	}

	buff := make([]interface{}, len(cols))
	// 存储每字段的值
	data := make([]string, len(cols))
	for i := range buff {
		buff[i] = &data[i]
	}
	for rows.Next() {
		rows.Scan(buff...)
	}

	// k 是索引，col 是字段对应的值
	for k, col := range data {
		for _, v := range items {
			if cols[k] == v {
				a := genMetric(col, p.mysqlIP, p.ptKey+"["+p.mysqlPort+`, "`+v+`"]`)
				//a := genMetric(col, p.mysqlIP, lldKey+"["+p.mysqlPort+", \""+keys[n]+"\"]")
				valueChan <- &bodyAndIP{body: a, ip: p.zbxAddr}
				break
			}
		}
	}

	//for k, col := range data {
	//	for n, v := range keys {
	//		if cols[k] == v {
	//			a := genMetric(col, p.mysqlIP, keys[n]+"["+p.mysqlPort+"]")
	//			//a := genMetric(col, p.mysqlIP, lldKey+"["+p.mysqlPort+", \""+keys[n]+"\"]")
	//			valueChan <- &bodyAndIP{body: a, ip: p.zbxAddr}
	//			break
	//		}
	//	}
	//}
}

func (p *allAttr) lldQuery(rows *sql.Rows) {
	if p.ptKey == "" {
		glog.Error("Missing configuration items under %s/%s/%s of configuration file",
			p.mysqlIP, p.mysqlPort, p.key)
		Exit(1)
	}

	var v1, v2, str string
	for rows.Next() {
		rows.Scan(&v1, &v2)
		a := genMetric(v2, p.mysqlIP, p.ptKey+"["+p.mysqlPort+`, "`+v2+`"]`)
		valueChan <- &bodyAndIP{body: a, ip: p.zbxAddr}
		str += v1 + " " + v2
	}
	glog.V(2).Infof("exec %s get value: %s", p.sql, str)

}

func (p *allAttr) getSQLValue() {
	//subCtx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second*2))
	//defer cancelFunc()
	rows, err := p.conn.Query(p.sql)
	if err != nil {
		glog.Errorf("Exec sql %s failed on %s %s, %v", p.sql, p.mysqlIP, p.mysqlPort, err)
		Exit(1)
	}
	defer rows.Close()
	switch p.flag {
	case "", "normal":
		p.normalQuery(rows)
	case "m/s":
		p.slaveStatus(rows)
	case "lld":
		p.lldQuery(rows)
	}
}

func (p allAttr) execSQL(t *time.Ticker) {
	p.getSQLValue()
	for {
		select {
		case _ = <-t.C:
			glog.V(1).Infof(fmt.Sprintf("start exec sql %s %s %s", p.mysqlIP, p.mysqlPort, p.key))
			p.getSQLValue()
		}
	}
}

func getPendingExecSql() {
	select {
	case <-notifyChan:
		glog.Warning("Start exec sql")
		for obj := range sqlChan {
			t := time.NewTicker(time.Duration(obj.frequency) * time.Second)
			go obj.execSQL(t)
			time.Sleep(time.Second * 2)
		}
	}
}

func genMetric(value, mysqlIP, key string) *zbxBody {
	t := time.Now().Unix()
	return &zbxBody{
		Host:  mysqlIP,
		Key:   key,
		Value: value,
		Clock: t,
	}
	//glog.V(2).Infof("Metric: %v", a)
	//valueChan <- &bodyAndIP{body: a, ip: zbxAddr}
}

func getLLDMsg(data *lowLevelDiscovery, mysqlIP, key string) *zbxBody {
	b, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		glog.Errorf("Get LLD json msg failed, %v", err)
		Exit(1)
	}
	return genMetric(string(b), mysqlIP, key)
}

func parseMap() {
	for mysqlIP, v := range allSqlConfig {
		msDiscovery := lowLevelDiscovery{}
		for port, mysqlConf := range v.Instances {
			conn := createMysqlConn(mysqlIP, port, mysqlConf.User, mysqlConf.Password, mysqlConf.Database)
			var count uint8
			for keys, _sql := range mysqlConf.SQL {
				sqlChan <- allAttr{
					zbxAddr:   v.ZbxAddr,
					mysqlIP:   mysqlIP,
					key:       keys,
					sql:       _sql.SQL,
					frequency: _sql.Frequency,
					conn:      conn,
					flag:      _sql.Flag,
					mysqlPort: port,
					ptKey:     _sql.PtKey,
					items:     _sql.Items,
				}
				if count > 1 {
					glog.Error("There can only be one master-slave monitoring under one instance")
					Exit(1)
				}
				if _sql.Flag == "m/s" {
					msDiscovery.Data = append(msDiscovery.Data, map[string]string{"{#PORT}": port})
					msg := getLLDMsg(&msDiscovery, mysqlIP, keys)
					msChan <- &bodyAndIP{body: msg, ip: v.ZbxAddr}
					count++
				}
			}
		}
	}
	close(sqlChan)
	close(msChan)
	//close(notifyChan)
}

func sendMetricToZbx(ip string, arrayBody []zbxBody) []byte {
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
	return resp
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
			for ip, b := range tmpMap {
				resp := sendMetricToZbx(ip, b)
				idx := bytes.Index(resp, []byte("failed:"))
				if string(resp[idx+8]) != "0" {
					i := bytes.Index(resp[idx+8:], []byte(";"))
					glog.Error("There are " + string(resp[idx+8:idx+8+i]) +
						" keys that failed to send, and the content sent is" +
						fmt.Sprintf("%v", b))
				}
			}
			return
		}
	}
}

func waitMetricToSend() {
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

func getLLDMsgFromChan() {
	for {
		select {
		case s := <-msChan:
			if s == nil {
				// 只有在低级发现都发送完毕的情况下，才能开始执行 SQL
				close(notifyChan)
				return
			}
			sendLLDMsg(s)
		}
	}
}

func sendLLDMsg(s *bodyAndIP) {
	for i := 0; i < 3; i++ {
		resp := sendMetricToZbx(s.ip, []zbxBody{*s.body})
		idx := bytes.Index(resp, []byte("failed:"))
		if string(resp[idx+8]) == "0" {
			return
		}
		glog.Error("Failed to send low-level discovery data")
		time.Sleep(2)
		continue
	}
	glog.Errorf("Sending three low-level discovery data failed, the program exits, data:", s.body)
	Exit(1)
}

func main() {
	loadYaml(getConfigFile())
	parseMap()
	select {}
}
