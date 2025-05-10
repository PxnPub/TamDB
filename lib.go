package TamDB;

import(
	OS       "os"
	Fmt      "fmt"
	Log      "log"
	SQL      "database/sql"
	Strings  "strings"
	UtilsSQL "github.com/PxnPub/pxnGoUtils/sql"
	_ "github.com/tursodatabase/go-libsql"
	_ "github.com/marcboeker/go-duckdb"
);



const DSN_LibSQL = "file:%s.sqlite?_pragma=journal_mode=MEMORY&_pragma=synchronous=NORMAL&mode=%s";
const DSN_DuckDB = "%s.duckdb?threads=2&memory_limit=1GB&mode=%s";
const PrefixValue = "V_";
const PrefixMerge = "M_";



type DriverType string;
const (
	Driver_LibSQL DriverType = "libsql"
	Driver_DuckDB DriverType = "duckdb"
);



type TamDB struct {
	Driver    DriverType
	DB        *SQL.DB
	RW        bool
	NameDB    string
	NameTable string
	PathDB    string
}



func New(driver DriverType, name_db string, name_table string,
		pathdb string, rw bool) (*TamDB, error) {
	// open database
	db, err := getDB(driver, name_db, name_table, pathdb, rw);
	if err != nil { return nil, err }
	return &TamDB{
		Driver:    driver,
		DB:        db,
		RW:        rw,
		NameDB:    name_db,
		NameTable: name_table,
		PathDB:    pathdb,
	}, nil;
}

func (tam *TamDB) Close() {
	if err := tam.DB.Close(); err != nil {
		Log.Printf("Error closing database: %v", err);
	}
}



func getDB(driver DriverType, name_db string, name_table string,
		pathdb string, rw bool) (*SQL.DB, error) {
	// create db dir
	if _, err := OS.Stat(pathdb); OS.IsNotExist(err) {
		if err := OS.Mkdir(pathdb, 0755); err != nil { return nil, err; }
	}
	var dsn      string;
	var timetype string;
	switch driver {
		case Driver_LibSQL: dsn = DSN_LibSQL; timetype = "INTEGER NOT NULL";
		case Driver_DuckDB: dsn = DSN_DuckDB; timetype = "TIMESTAMP";
	}
	var mode string;
	if rw  { mode = "rwc";
	} else { mode = "ro"; }
	// open database
	db, err := SQL.Open(string(driver), Fmt.Sprintf(dsn, pathdb+name_db, mode));
	if err != nil                   { return nil, err; }
	if rw  { db.SetMaxOpenConns( 1); db.SetMaxIdleConns(1);
	} else { db.SetMaxOpenConns(10); db.SetMaxIdleConns(5); }
	if err := db.Ping(); err != nil { return nil, err; }
	if rw {
		// create table if needed
		sql := Fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS "%s" (time %s, PRIMARY KEY (time) );`,
			name_table,
			timetype,
		);
		if _, err := db.Exec(sql); err != nil { return nil, err; }
	}
	return db, nil;
}



func (tam *TamDB) AddField(key string) {
	if !tam.RW { panic("Cannot AddField() to read-only database"); }
	// value field
	{
		var fieldtype string;
		switch tam.Driver {
			case Driver_LibSQL: fieldtype = "INTEGER NOT NULL";
			case Driver_DuckDB: fieldtype = "BIGINT";
		}
		sql := Fmt.Sprintf(
			`ALTER TABLE "%s" ADD COLUMN "%s%s" %s DEFAULT 0;`,
			tam.NameTable,
			PrefixValue,
			key,
			fieldtype,
		);
		if _, err := tam.DB.Exec(sql); err != nil && !UtilsSQL.IsDuplicateColumnError(err) {
			Fmt.Printf("%s\n%s\n", err, sql);
		}
	}
	// merge field
	{
		var fieldtype string;
		switch tam.Driver {
			case Driver_LibSQL: fieldtype = "INTEGER NOT NULL";
			case Driver_DuckDB: fieldtype = "SMALLINT";
		}
		sql := Fmt.Sprintf(
			`ALTER TABLE "%s" ADD COLUMN "%s%s" %s DEFAULT 0;`,
			tam.NameTable,
			PrefixMerge,
			key,
			fieldtype,
		);
		if _, err := tam.DB.Exec(sql); err != nil && !UtilsSQL.IsDuplicateColumnError(err) {
			Fmt.Printf("%s\n%s\n", err, sql);
		}
	}
}



func (tam *TamDB) Submit(time int64, key string, value int64) {
	if !tam.RW { panic("Cannot AddField() to read-only database"); }
	var timetype string;
	switch tam.Driver {
		case Driver_LibSQL: timetype = "?";
		case Driver_DuckDB: timetype = "TO_TIMESTAMP(?)";
	}
	tx, err := tam.DB.Begin();
	if err != nil { Log.Panic(err); }
	defer func() {
		if rec := recover(); rec != nil {
			tx.Rollback();
			Log.Panic(rec);
		} else {
			if err := tx.Commit(); err != nil {
				Log.Panic(err);
			}
		}
	}();
	// insert row
	{
		sql := Fmt.Sprintf(
			`INSERT INTO "%s" (time) VALUES ( %s ) ON CONFLICT (time) DO NOTHING;`,
			tam.NameTable,
			timetype,
		);
		stmt, err := tx.Prepare(sql);
		if err != nil { Log.Panic(err); }
		if _, err := stmt.Exec(time); err != nil { Log.Panic(err); }
		stmt.Close();
	}
	// update row
	{
		sql := Fmt.Sprintf(
			`UPDATE "%s" SET "%s%s"="%s%s"+?, "%s%s"="%s%s"+1 WHERE time=%s;`,
			tam.NameTable,
			PrefixValue, key, PrefixValue, key,
			PrefixMerge, key, PrefixMerge, key,
			timetype,
		);
		stmt, err := tx.Prepare(sql);
		if err != nil { Log.Panic(err); }
		if _, err := stmt.Exec(value, time); err != nil { Log.Panic(err); }
		stmt.Close();
	}
}



func SplitNameKey(name string) (string, string) {
	if Strings.Contains(name, ":") {
		parts := Strings.SplitN(name, ":", 2);
		return parts[0], parts[1];
	}
	return name, name;
}
