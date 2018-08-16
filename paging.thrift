namespace go mapper

struct Paging {
	1: i32 RowsPerPage ,
	2: i32 OnPage,
	3: i32 AllRows,
	4: i32 AllPages,
	5: i32 NextPage,
	6: i32 LastPage,
	7: i32 OffSet
}

/***
    RowsPerPage 每页显示行数
    OnPage      当前页
    AllRows     总行数
    AllPages    总页数
    NextPage    下一页
    LastPage    上一页
    OffSet      偏移量
***/
