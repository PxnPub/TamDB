package server;

import(
	Log      "log"
	Fmt      "fmt"
	Sync     "sync"
	Time     "time"
	Net      "net"
	RPC      "net/rpc"
	Errors   "errors"
	TamDB    "github.com/PxnPub/TamDB"
	API      "github.com/PxnPub/TamDB/api"
	TrapC    "github.com/PxnPub/pxnGoUtils/trapc"
	UtilsNet "github.com/PxnPub/pxnGoUtils/net"
);



//const DefaultSocket = "unix:///run/tamdb.socket";
//const DefaultSocket = "tcp://127.0.0.1:9999";



type TamAPI struct {
	StopChan chan bool
	WaitGrp  *Sync.WaitGroup
	Tams     map[string]TamDB.TamDB
	Bind     string
	Listen   *Net.Listener
}



func New(trapc *TrapC.TrapC, bind string) *TamAPI {
	return &TamAPI{
		StopChan: trapc.NewStopChan(),
		WaitGrp:  trapc.WaitGrp,
		Tams:     make(map[string]TamDB.TamDB),
		Bind:     bind,
	};
}

func (api *TamAPI) Close() {
	if api.Listen != nil {
		(*api.Listen).Close();
		api.Listen = nil;
	}
}



func (api *TamAPI) AddTamDB(tam *TamDB.TamDB, name_db string) {
	api.Tams[name_db] = *tam;
}



func (api *TamAPI) StartListening() {
	// api socket
	listen, err := UtilsNet.NewSock(api.Bind);
	if err != nil { panic(err); }
	api.Listen = listen;
	// rpc
	if err := RPC.Register(api); err != nil { panic(err); }
	go func() {
		api.WaitGrp.Add(1);
		defer func() {
			api.Close();
			api.WaitGrp.Done();
		}();
		Fmt.Printf("API listening: %s\n", api.Bind);
		timeout := Time.Duration(200) * Time.Millisecond;
		for {
			select {
				case stopping := <-api.StopChan:
					if stopping {
						Log.Print(" [ API ] Stopping listener..");
						return;
					}
				default:
			}
			(*api.Listen).(*Net.TCPListener).SetDeadline(Time.Now().Add(timeout));
			conn, err := (*api.Listen).Accept();
			if err == nil {
				go RPC.ServeConn(conn);
			} else {
				neterr, ok := err.(Net.Error);
				if !ok || !neterr.Timeout() {
					Log.Printf("Connection failed: %v", err);
				}
			}
		}
	}();
	Time.Sleep(Time.Duration(250) * Time.Millisecond);
}

func (api *TamAPI) Query(request API.Query, reply *API.QueryReply) error {
	if request.NameDB    == "" { return Errors.New("Invalid database name"); }
	if request.NameTable == "" { return Errors.New("Invalid table name"   ); }
	tam, ok := api.Tams[request.NameDB];
	if !ok { return Fmt.Errorf("Invalid database name: %s", request.NameDB); }
	tx, err := tam.DB.Begin();
	if err != nil { Log.Print(err); return err; }
//"SELECT `time`, `%s%s`, `%s%s` FROM `%s` WHERE `time` > UNIXEPOCH()-3600",
	sql := Fmt.Sprintf(
		"SELECT `time`, `%s%s`, `%s%s` FROM `%s`",
		TamDB.PrefixValue, request.FieldKey,
		TamDB.PrefixMerge, request.FieldKey,
		request.NameTable,
	);
	stmt, err := tx.Prepare(sql);
	if err != nil { Log.Print(err); return err; }
	defer stmt.Close();
	data_x := make([]int64, 0);
	data_y := make([]int64, 0);
	rows, err := stmt.Query();
	if err != nil { Log.Print(err); return err; }
	defer rows.Close();
	for rows.Next() {
		var timestamp int64;
		var value     int64;
		var merge     int64;
		if err := rows.Scan(&timestamp, &value, &merge); err != nil {
			Log.Print(err); return err; }
		val := value / merge;
		data_x = append(data_x, timestamp);
		data_y = append(data_y, val      );
	}
	reply.DataX = data_x;
	reply.DataY = data_y;
	// commit
	if err := tx.Commit(); err != nil { Log.Print(err); return err; }
	return nil;
}
