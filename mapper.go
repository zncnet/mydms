package md

import (
	"database/sql"
	"math"
	"sync"

	_ "github.com/lib/pq"
)

const (
	DB_NAME   = "postgres"
	DB_HOST   = "localhost"
	DB_PORT   = "5432"
	DB_DRIVER = "postgres"
	DB_USER   = "postgres"
	DB_PAWD   = "postgres"
)

type Mapper struct {
	DB *sql.DB
}

var _dbConnStr = "host=" + DB_HOST + " port=" + DB_PORT + " user=" + DB_USER + " password=" + DB_PAWD + " dbname=" + DB_NAME + " sslmode=disable client_encoding=UTF8"

// 获取数据库连接池
func ConnectDB(db_driver, db_connStr string) *Mapper {
	db, err := sql.Open(db_driver, db_connStr)
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}
	db.SetMaxIdleConns(5)   //闲置
	db.SetMaxOpenConns(100) //最大
	return &Mapper{db}
}

func (m *Mapper) Exec(sqlStatement string, args ...interface{}) (int64, error) {
	stmt, err := m.DB.Prepare(sqlStatement)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (m *Mapper) Query(sqlStatement string, args ...interface{}) (*sql.Rows, error) {

	stmt, err := m.DB.Prepare(sqlStatement)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	return rows, err
}

func (m *Mapper) QueryRow(sqlStatement string, args ...interface{}) (*sql.Row, error) {

	stmt, err := m.DB.Prepare(sqlStatement)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	row := stmt.QueryRow(args...)
	return row, nil
}

var _conn *Mapper

var _once sync.Once

func GetMapper() *Mapper {
	_once.Do(func() {
		_conn = ConnectDB(DB_DRIVER, _dbConnStr)
	})
	return _conn
}

type Paging struct {
	RowsPerPage int `每页显示行数`
	OnPage      int `当前页`
	AllRows     int `总行数`
	AllPages    int `总页数`
	NextPage    int `下一页`
	LastPage    int `上一页`
	OffSet      int `偏移量`
}

const DEFAULT_ROWS_PER_PAGE = 20 //默认每页显示行数

func (p *Paging) Setup(n int) {
	if p.OnPage <= 0 {
		p.OnPage = 1
	}
	if p.RowsPerPage <= 0 {
		p.RowsPerPage = DEFAULT_ROWS_PER_PAGE
	}
	p.AllRows = n
	p.AllPages = int(math.Ceil(float64(p.AllRows) / float64(p.RowsPerPage)))
	p.OffSet = (p.OnPage - 1) * p.RowsPerPage
	if p.OnPage < p.AllPages {
		p.NextPage = p.OnPage + 1
	}
	if p.OnPage > 1 {
		p.LastPage = p.OnPage - 1
	}
}
