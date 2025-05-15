package TamDB;

import(
	OS       "os"
	Fmt      "fmt"
	Log      "log"
	SQL      "database/sql"
	Strings  "strings"
	UtilsSQL "github.com/PxnPub/pxnGoUtils/sql"
	_ "github.com/tursodatabase/go-libsql"
	_ "github.com/marcboeker/go-duckdb/v2"
);



const DSN_LibSQL = "file:%s.sqlite?_pragma=journal_mode=MEMORY&_fk=true&_pragma=synchronous=NORMAL&mode=%s";
const DSN_DuckDB = "%s.duckdb?threads=2&memory_limit=1GB";
const PrefixMerge = "M_";
const PrefixValue = "V_";



type DriverType string;
const (
	Driver_LibSQL DriverType = "libsql"
	Driver_DuckDB DriverType = "duckdb"
);



type TamDB struct {
	Driver       DriverType
	DB           *SQL.DB
	RW           bool
	MasterMerges bool
	NameDB       string
	NameTable    string
	PathDB       string
}



func New(driver DriverType, name_db string, name_table string,
		pathdb string, rw bool, mastermerges bool) (*TamDB, error) {
	// open database
	db, err := getDB(driver, name_db, name_table, pathdb, rw, mastermerges);
	if err != nil { return nil, err }
	return &TamDB{
		Driver:       driver,
		DB:           db,
		RW:           rw,
		MasterMerges: mastermerges,
		NameDB:       name_db,
		NameTable:    name_table,
		PathDB:       pathdb,
	}, nil;
}

func (tam *TamDB) Close() {
	if err := tam.DB.Close(); err != nil {
		Log.Printf("Error closing database: %v", err);
	}
}



//TODO: should time field be TIMESTAMP type or INTEGER? what has best performance in DuckDB?
func getDB(driver DriverType, name_db string, name_table string,
		pathdb string, rw bool, mastermerges bool) (*SQL.DB, error) {
	// create db dir
	if _, err := OS.Stat(pathdb); OS.IsNotExist(err) {
		if err := OS.Mkdir(pathdb, 0755); err != nil { return nil, err; }
	}
	var mode string;
	if rw { mode = "rwc"; } else { mode = "ro"; }
	var dsn      string;
	var timetype string;
	switch driver {
		case Driver_LibSQL:
			dsn = Fmt.Sprintf(DSN_LibSQL, pathdb+name_db, mode);
			timetype = "INTEGER NOT NULL";
		case Driver_DuckDB:
			dsn = Fmt.Sprintf(DSN_DuckDB, pathdb+name_db);
//			timetype = "TIMESTAMP";
			timetype = "INTEGER";
	}
	// open database
	db, err := SQL.Open(string(driver), dsn);
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
		// merge field
		if mastermerges {
			var fieldtype string;
			switch driver {
				case Driver_LibSQL: fieldtype = "INTEGER NOT NULL";
				case Driver_DuckDB: fieldtype = "SMALLINT";
			}
			sql := Fmt.Sprintf(
				`ALTER TABLE "%s" ADD COLUMN "merges" %s DEFAULT 0;`,
				name_table,
				fieldtype,
			);
			if _, err := db.Exec(sql); err != nil &&
			!UtilsSQL.IsDuplicateColumnError(err) {
				Fmt.Printf("%s\n%s\n", err, sql);
			}
		}
	}
	return db, nil;
}



func (tam *TamDB) AddField(key string) {
	if !tam.RW { panic("Cannot AddField() to read-only database"); }
	// merge field
	if ! tam.MasterMerges {
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
}



func (tam *TamDB) Submit(time int64, key string, value int64) {
	if !tam.RW { panic("Cannot AddField() to read-only database"); }
	var timetype string;
timetype = "?";
//	switch tam.Driver {
//		case Driver_LibSQL: timetype = "?";
//		case Driver_DuckDB: timetype = "TO_TIMESTAMP(?)";
//	}
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
			`INSERT INTO "%s" (time) VALUES (%s) ON CONFLICT (time) DO NOTHING;`,
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
