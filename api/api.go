package api;



type Query struct {
	NameDB    string
	NameTable string
	FieldKey  string
}

type QueryReply struct {
	DataX []int64
	DataY []int64
}
