'use babel'

const fs = require('fs')
const { Client } = require('pg')
const path = require('path')
const mustache = require('mustache')


export default {

    generate(path, dir) {
        if (!path) return
        let data = fs.readFileSync(path, 'utf8')
        let info = {}
        try {
            info = JSON.parse(data + "")
            if (this.checkInfo(info)) {
                this.dbHandler(info, dir)
            } else {
                atom.notifications.addWarning(bar + "\n\n不是mydms配置文件或配置错误")
            }
        } catch (e) {
            atom.notifications.addWarning("mydms配置文件必须是json文件")
        }
    },

    checkInfo(info) {
        if (info) {
            if (info.db_info) {
                return true
            }
        }
        return false
    },

    dbHandler: async function(info, dir) {

        const client = new Client(info.db_info)
        await client.connect().then(() => {})
            .catch(e => atom.notifications.addWarning("Mydms : Datebase Connect Faild!"))

        for (let k in info.tb_list) {
            let tb = info.tb_list[k]

            let res = await client.query(sqlTbInfo, [tb.tableName, tb.schema ? tb.schema : 'public'])
            let keyInfo = await client.query(sqlKeyInfo, [tb.tableName, tb.schema ? tb.schema : 'public'])
            let keys = []


            if (!!tb.keys) {
                for (let i in tb.keys) {
                    keys.push(tb.keys[i])
                }
            }

            var arCnt = function(arr, k) {
                for (let i in arr) {
                    if (arr[i] == k) {
                        return true
                    }
                }
                return false
            }

            if (!!keyInfo.rows) {
                for (let i in keyInfo.rows) {
                    if (!arCnt(keys, keyInfo.rows[i].colname)) {
                        keys.push(keyInfo.rows[i].colname)
                    }
                }
            }

            let keyCount = keys.length

            res.className = tb.className
            res.tableName = tb.tableName
            res.package = 'main' //TODO ...
            let nameArray = []
            let fieldArr1 = []
            let fieldArr2 = []
            let fieldArr3 = []
            let slotArray = []
            let updateArr = []
            let keyWhere1 = []
            let keyWhere2 = []
            let kprsArray = []
            let kpnsArray = []
            let ktrsArray = []

            let rowCount = res.rows.length

            let kia = 0
            let kib = rowCount - keyCount

            let index = 0
            let upi = 0

            res.kfields = ''
            for (let i in res.rows) {
                res.rows[i].index = parseInt(i) + 1
                res.rows[i].field = this.transName(res.rows[i].name)
                nameArray.push(res.rows[i].name)
                fieldArr1.push('&obj.' + res.rows[i].field)
                fieldArr2.push('obj.' + res.rows[i].field)
                index++
                slotArray.push('$' + index)

                res.rows[i].type = this.transType(res.rows[i].pgtype)
                res.rows[i].thriftType = this.transThriftType(res.rows[i].pgtype)
                res.rows[i].flag = i != (rowCount-1) ? "," : ""

                res.rows[i].isKey = false
                for (let j in keys) {
                    if (keys[j] == res.rows[i].name) {
                        res.rows[i].isKey = true
                        kia++
                        kib++
                        keyWhere1.push(res.rows[i].name + " = $" + kia)
                        keyWhere2.push(res.rows[i].name + " = $" + kib)
                        kpnsArray.push(res.rows[i].field)
                        kprsArray.push(res.rows[i].field + " " + res.rows[i].type)
                        ktrsArray.push(kia + ":" + res.rows[i].thriftType + " " + res.rows[i].field)
                        res.kfields += ', obj.' + res.rows[i].field
                    }
                }
                if (!res.rows[i].isKey) {
                    upi++
                    updateArr.push(res.rows[i].name + " = $" + upi)
                    fieldArr3.push('obj.' + res.rows[i].field)
                }
            }

            res.names = nameArray.join(", ")
            res.fields1 = fieldArr1.join(", ")
            res.fields2 = fieldArr2.join(", ")
            res.fields3 = fieldArr3.join(", ")
            res.slots = slotArray.join(", ")
            res.updates = updateArr.join(", ")
            res.kw1 = keyWhere1.join(" AND ")
            res.kw2 = keyWhere2.join(" AND ")
            res.kprs = kprsArray.join(", ")
            res.kpns = kpnsArray.join(", ")
            res.ktrs = ktrsArray.join(", ")

            let bar = dir
            if (!bar) return
            if (info.md_path.length > 0) {
                res.package = path.basename(info.md_path)
                bar += "/" + info.md_path
                this.mkdirs(bar)
            }

            let gfs = bar + "/" + res.className + "Mapper" + ".go"

            let output = mustache.render(golangObjectTmp, res)
            //output += "/*" + JSON.stringify(res.rows, null, 4) + "*/"
            fs.open(gfs, 'wx', (err, fd) => {
                if (err) {
                    if (err.code === 'EEXIST') {
                        atom.workspace.open(bar)
                        return
                    }
                    throw err
                }
                fs.write(fd, output, 0, 'utf-8', (err, written, string) => {
                    if (err) throw err
                })
                fs.close(fd, (err) => {
                    if (err) throw err
                })
                atom.workspace.open(gfs)
            })

            let thriftdir = res.thriftdir ? res.thriftdir : "thriftdir"
            let tfs = dir + "/" + thriftdir + "/" + res.className + "Service.thrift"
            let tfsopt = mustache.render(thriftObjectTmp, res)

            fs.open(tfs, 'wx', (err, fd) => {
                if (err) {
                    if (err.code === 'EEXIST') {
                        atom.workspace.open(bar)
                        return
                    }
                    throw err
                }
                fs.write(fd, tfsopt, 0, 'utf-8', (err, written, string) => {
                    if (err) throw err
                })
                fs.close(fd, (err) => {
                    if (err) throw err
                })
                atom.workspace.open(tfs)
            })
        }
        await client.end()
    },

    async mkdirs(dirpath) {
        if (!fs.existsSync(path.dirname(dirpath))) {
            this.mkdirs(path.dirname(dirpath));
        }
        await fs.mkdirSync(dirpath)
    },

    transName(str) {
        if (str.length == 0) {
            return str
        }
        str = str.replace(/\_[a-z]/g, function(a, b) {
            return b == 0 ? a.replace('_', '') : a.replace('_', '').toUpperCase()
        })
        return str.substring(0, 1).toUpperCase() + str.substring(1)
    },

    transType(t) {
        switch (t) {
            case 'bit':
                return 'byte'
            case 'varbit':
                return '[]byte'
            case 'bool':
                return 'bool'
            case 'int2':
            case 'serial2':
                return 'int16'
            case 'int':
            case 'int4':
            case 'serial4':
                return 'int'
            case 'int8':
            case 'serial8':
                return 'int64'
            case 'float4':
            case 'float8':
                return 'float32'
            case 'money':
            case 'decimal':
            case 'numeric':
                return 'float64'
            case 'char':
            case 'text':
            case 'varchar':
                return 'string'
            case 'date':
            case 'time':
            case 'timetz':
            case 'timestamp':
            case 'timestamptz':
                return 'time.Time'
            default:
                return 'string'
        }
    },

    transThriftType(t) {
        switch (t) {
            case 'bit':
                return 'byte'
            case 'varbit':
                return 'binary'
            case 'bool':
                return 'bool'
            case 'int2':
            case 'serial2':
                return 'i16'
            case 'int':
            case 'int4':
            case 'serial4':
                return 'i32'
            case 'int8':
            case 'serial8':
                return 'i64'
            case 'float4':
            case 'float8':
                return 'double'
            case 'money':
            case 'decimal':
            case 'numeric':
                return 'double'
            case 'char':
            case 'text':
            case 'varchar':
                return 'string'
            case 'date':
            case 'time':
            case 'timetz':
            case 'timestamp':
            case 'timestamptz':
                return 'string'
            default:
                return 'string'
        }
    }
}

const jsonContentTmp = `{
    "db_info": {
        "user": "postgres",
        "host": "localhost",
        "database": "databaseName",
        "password": "postgres",
        "port": 5432
    },
    "md_path": "md",
    "tb_list": [{
        "schema": "public",
        "tableName": "test",
        "className": "Test",
        "keys": ["id"]
    }]
}`

var golangObjectTmp = "package {{package}}\r\n\r\n" +
    "import (\r\n" +
    "    \"git.apache.org/thrift.git/lib/go/thrift\"\r\n" +
    ")\r\n" +

    "/*type {{className}} struct {\r\n" +
    "{{#rows}}    {{field}}\t{{type}}\t`{{comment}}`\r\n{{/rows}}" +
    "}*/\r\n"

var thriftObjectTmp = `namespace go {{package}}

include "paging.thrift"

struct {{className}} {
{{#rows}}
    {{index}}: {{thriftType}} {{field}}{{flag}}
{{/rows}}
}

service {{className}}Service{

    list<{{className}}> QueryP(1:paging.Paging page)

    i64 Insert(1:{{className}} obj)

    i64 Delete({{ktrs}})

    i64 Update(1:{{className}} obj)

    {{className}} QueryR({{ktrs}})

    list<{{className}}> QueryL()

}

`

golangObjectTmp += `
func (obj *{{className}}) Mapper() *{{className}}Mapper {
	return &{{className}}Mapper{GetMapper()}
}

type {{className}}Mapper struct {
	*Mapper
}

func (p *{{className}}Mapper) QueryP(ctx context.Context, page *Paging) (r []*{{className}}, err error) {
	sqlWhere := " where 1=1"
	args := make([]interface{}, 0)
	row, err := p.QueryRow("select count(*) from {{tableName}}"+sqlWhere, args...)
	if err != nil {
		return nil, err
	}
	count := 0
	err = row.Scan(&count)
	if err != nil {
		return nil, err
	}
	page.Setup(count)
	args = append(args, page.RowsPerPage)
	args = append(args, page.OffSet)
	rows, err := p.Query("select {{names}} from {{tableName}}"+sqlWhere+" limit $1 offset $2", args...)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	r = make([]*{{className}}, 0)
	for rows.Next() {
		obj := &{{className}}{}
		err = rows.Scan({{{fields1}}})
		if err != nil {
			return nil, err
		}
		r = append(r, obj)
	}
	return r, err
}

func (p *{{className}}Mapper) Insert(ctx context.Context, obj *{{className}}) (r int64, err error) {
	sql := "INSERT INTO {{tableName}}({{names}}) Values({{{slots}}})"
	return p.Exec(sql, {{fields2}})
}

func (p *{{className}}Mapper) Delete(ctx context.Context, {{kprs}}) (r int64, err error) {
	sql := "DELETE FROM {{tableName}} WHERE {{{kw1}}}"
	return p.Exec(sql, {{kpns}})
}

func (p *{{className}}Mapper) Update(ctx context.Context, obj *{{className}}) (r int64, err error) {
	sql := "UPDATE {{tableName}} SET {{{updates}}} WHERE {{{kw2}}}"
	return p.Exec(sql, {{fields3}}{{kfields}})
}

func (p *{{className}}Mapper) QueryR(ctx context.Context, {{kprs}}) (obj {{className}}, err error) {
	sql := "select {{names}} from {{tableName}} where {{{kw1}}} limit 1"
	row, err := p.QueryRow(sql, {{kpns}})
	if err != nil {
		return obj, err
	}
	err = row.Scan({{{fields1}}})
	return obj, err
}

func (p *{{className}}Mapper) QueryL(ctx context.Context) (r []*{{className}}, err error) {
	sql := "select {{names}} from {{tableName}}"
	rows, err := p.Query(sql)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	r = make([]*{{className}}, 0)
	for rows.Next() {
		obj := &{{className}}{}
		err = rows.Scan({{{fields1}}})
		if err != nil {
			return nil, err
		}
		r = append(r, obj)
	}
	return r, err
}
`

// 表、视图列名（数据类型为别名）
const sqlTbInfo = `
    SELECT col_description(a.attrelid,a.attnum) as comment,
         pg_type.typname as pgtype,
         a.attname as name,
         a.attnotnull as notnull
    FROM pg_class as c
            inner join pg_namespace d on d.oid = c.relnamespace,
         pg_attribute as a
            inner join pg_type on pg_type.oid = a.atttypid
    WHERE c.relname = $1 and d.nspname= $2 and a.attrelid = c.oid and a.attnum>0`

// 主键信息
const sqlKeyInfo = `select
        pg_constraint.conname as pk_name,
        pg_attribute.attname as colname,
        pg_type.typname as pgtype
        from
        pg_constraint
        inner join pg_class on pg_constraint.conrelid = pg_class.oid
        inner join pg_namespace as d on d.oid = pg_class.relnamespace
        inner join pg_attribute on pg_attribute.attrelid = pg_class.oid  and  pg_attribute.attnum = any(pg_constraint.conkey)
        inner join pg_type on pg_type.oid = pg_attribute.atttypid
        where pg_class.relname = $1
        and d.nspname= $2
        and pg_constraint.contype='p'
        and pg_table_is_visible(pg_class.oid)`
