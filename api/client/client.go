package client;

import(
	RPC "net/rpc"
	API "github.com/PxnPub/TamDB/api"
);



type TamAPI struct {
	Client *RPC.Client
}



func New(protocol string, address string) (*TamAPI, error) {
	// connect rpc
	rpc := RPC.NewServer();
	client, err := rpc.Dial(protocol, address);
	if err != nil { return nil, err; }
	return &TamAPI{
		Client: client,
	}, nil;
}

func (api *TamAPI) Close() {
	api.Client.Close();
}



func (api *TamAPI) Query(name_db string, name_table string, field_key string) *API.QueryReply {
	request := API.Query{
		name_db,
		name_table,
		field_key,
	};
	var reply API.QueryReply;
	if err := api.Client.Call("TamAPI.Query", request, &reply); err != nil { panic(err); }
	return &reply;
}
