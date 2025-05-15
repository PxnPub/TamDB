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
	Socket   Net.Listener
}



func New(trapc *TrapC.TrapC, bind string) *TamAPI {
	return &TamAPI{
		StopChan: trapc.NewStopChan(),
		WaitGrp:  trapc.WaitGrp,
		Tams:     make(map[string]TamDB.TamDB),
		Bind:     bind,
		Socket:   nil,
	};
}

func (api *TamAPI) Close() {
	if api.Socket != nil {
		api.Socket.Close();
		api.Socket = nil;
	}
}



func (api *TamAPI) AddTamDB(tam *TamDB.TamDB, name_db string) {
	api.Tams[name_db] = *tam;
}



func (api *TamAPI) Listen() {
	// api socket
	listen, err := UtilsNet.NewSock(api.Bind);
	if err != nil { panic(err); }
	api.Socket = *listen;
	// rpc
	rpc := RPC.NewServer();
	if err := rpc.Register(api); err != nil { panic(err); }
	go func() {
		api.WaitGrp.Add(1);
		defer func() {
			api.Close();
			api.WaitGrp.Done();
		}();
		Log.Printf("[ API ] Listening: %s\n", api.Bind);
		listentimeout := Time.Duration(200) * Time.Millisecond;
		for {
			select {
			case stopping := <-api.StopChan:
				if stopping {
					Log.Print(" [ API ] Stopping listener..");
					return;
				}
			default:
			}
			api.Socket.(*Net.TCPListener).
				SetDeadline(Time.Now().Add(listentimeout));
			conn, err := api.Socket.Accept();
			if err == nil {
				go rpc.ServeConn(conn);
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
	var timetype string;
timetype = `"time"`;
//	switch tam.Driver {
//		case TamDB.Driver_LibSQL: timetype = `"time"`;
//		case TamDB.Driver_DuckDB: timetype = `CAST(EXTRACT(EPOCH FROM time) AS INTEGER) AS "time"`;
//	}
//	tx, err := tam.DB.Begin();
//	if err != nil { Log.Print(err); return err; }

	sql := Fmt.Sprintf(
//		`SELECT %s, "%s%s", "%s%s" FROM "%s" ORDER BY "time" ASC`,
		`SELECT %s, "%s%s", "%s%s" FROM "%s" WHERE "time" > ? ORDER BY "time" ASC`,
//		`SELECT %s, "%s%s", "%s%s" FROM "%s" WHERE "time" > UNIXEPOCH() - 3600 ORDER BY "time" ASC`,
//		`SELECT %s, "%s%s", "%s%s" FROM "%s" ORDER BY "time" ASC`,
// WHERE time BETWEEN ? AND ?
		timetype,
		TamDB.PrefixValue, request.FieldKey,
		TamDB.PrefixMerge, request.FieldKey,
		request.NameTable,
	);
//	stmt, err := tx.Prepare(sql);
	stmt, err := tam.DB.Prepare(sql);
	if err != nil { Log.Print(err); return err; }
	defer stmt.Close();
	data_x := make([]int64, 0);
	data_y := make([]int64, 0);
	rows, err := stmt.Query(Time.Now().Unix() - 43200);
//	rows, err := stmt.Query(Time.Now().Unix() - 86400);
//	rows, err := stmt.Query();
	if err != nil { Log.Print(err); return err; }
	defer rows.Close();
	for rows.Next() {
		var timestamp int64;
		var value     int64;
		var merge     int64;
		if err := rows.Scan(&timestamp, &value, &merge); err != nil {
			Log.Print(err); return err; }
		var val int64;
		if merge > 0 { val = value / merge;
		} else {       val = 0; }
		data_x = append(data_x, timestamp);
		data_y = append(data_y, val      );
	}
	reply.DataX = data_x;
	reply.DataY = data_y;
//	reply.DataY = breaklineswhenxincreasesby(data_x, data_y, 20);
	// commit
//	if err := tx.Commit(); err != nil { Log.Print(err); return err; }
	return nil;
}

//func breaklineswhenxincreasesby(values_x []int64, values_y []int64, threshold int64) []int64 {
//	modified_values_y := make([]int64, len(values_y));
//	for i := range values_y {
//		if i > 0 && (values_x[i] - values_x[i-1] > threshold) {
//			modified_values_y[i] = 0;
//		} else {
//			modified_values_y[i] = values_y[i];
//		}
//	}
//	return modified_values_y;
//}
