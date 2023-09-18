package main

import (
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strconv"
	"strings"
)

func GetMysqlTableDDL(db *gorm.DB, tableName string) string {
	var tb string
	var ddl string
	if err := db.Raw("SHOW CREATE TABLE `"+tableName+"`").Row().Scan(&tb, &ddl); err != nil {
		panic("获取表结构失败" + err.Error())
	}
	fmt.Println("--------------原表结构ddl----------------------")
	fmt.Println(ddl)
	fmt.Println("----------------------------------------------------\n")
	return ddl
}

type ColumnInfo struct {
	ColumnName    string `gorm:"column:COLUMN_NAME"`
	ColumnDefault string `gorm:"column:COLUMN_DEFAULT"`
	IsNullable    string `gorm:"column:IS_NULLABLE"`
	ColumnType    string `gorm:"column:COLUMN_TYPE"`
	DataType      string `gorm:"column:DATA_TYPE"`
	ColumnKey     string `gorm:"column:COLUMN_KEY"`
	ColumnComment string `gorm:"column:COLUMN_COMMENT"`
}

func GetAllColumnInfo(db *gorm.DB, dbName string, tbName string) []*ColumnInfo {
	var columns []*ColumnInfo
	query := fmt.Sprintf("SELECT COLUMN_NAME,COLUMN_TYPE, DATA_TYPE, COLUMN_COMMENT,COLUMN_KEY,IS_NULLABLE,COLUMN_DEFAULT "+
		"FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", dbName, tbName)
	if err := db.Raw(query).Scan(&columns).Error; err != nil {
		panic("无法获取表的字段元数据: " + err.Error())
	}
	return columns
}

// BuildDorisDDLStatement 根据mysql表的字段信息构建doris表结构
func BuildDorisDDLStatement(columns []*ColumnInfo, tbName string) (string, string) {
	ddl := "CREATE TABLE IF NOT EXISTS `" + tbName + "` (\n"
	uniqueKey := "请手动指定unique key"
	for i, column := range columns {
		if column.ColumnKey == "PRI" || column.ColumnKey == "AUTO_INCREMENT" || column.ColumnKey == "UNI" {
			uniqueKey = column.ColumnName
		}
		ddl += fmt.Sprintf("  %-25s%-15s", "`"+column.ColumnName+"`", TransField2Doris(column.DataType, column.ColumnType))
		if column.ColumnComment != "" {
			ddl += fmt.Sprintf("COMMENT %s", "`"+column.ColumnComment+"`")
		}
		if i < len(columns)-1 {
			ddl += ",\n"
		} else {
			ddl += "\n"
		}
	}
	ddl += ") ENGINE=OLAP\nUNIQUE KEY(`" + uniqueKey + "`)\n" +
		"COMMENT 'OLAP'\n" +
		"DISTRIBUTED BY HASH(`" + uniqueKey + "`) BUCKETS 3\n" +
		"PROPERTIES (\n  \"" +
		"replication_allocation\" = \"tag.location.default: 3\",\n  \"" +
		"in_memory\" = \"false\",\n  \"" +
		"storage_format\" = \"V2\",\n  \"" +
		"disable_auto_compaction\" = \"false\"\n);"
	fmt.Println("--------------DORIS表结构ddl----------------------")
	fmt.Println(ddl)
	fmt.Println("---------------------------------------------------\n")
	return ddl, uniqueKey
}

func BuildFlinkSQL(columns []*ColumnInfo, uniqueKey string) string {
	res := ""
	prefix := BuildDinkyPrefix()
	source := BuildDinkySource(BuildDinkyFieldWithType(columns), uniqueKey)
	sink := BuildDinkySink(BuildDinkyFieldWithType(columns))
	query := BuildDinkyJob(columns)
	res = prefix + "\n" + source + "\n" + sink + "\n" + query
	fmt.Println("--------------FlinkSQL----------------------")
	fmt.Println(res)
	fmt.Println("---------------------------------------------------\n")
	return res
}

func BuildDinkyPrefix() string {
	res := "SET execution.checkpointing.externalized-checkpoint-retention = RETAIN_ON_CANCELLATION;\n" +
		"SET execution.checkpointing.interval = 30s;\n" +
		"DROP TABLE IF EXISTS cdc_test_source;\n" +
		"DROP TABLE IF EXISTS doris_test_sink;\n"
	return res
}

func BuildDinkySource(fields string, uniqueKey string) string {
	res := "CREATE TABLE `cdc_test_source` (\n" +
		fields + ",\n" +
		"  PRIMARY KEY (`" + uniqueKey + "`) NOT ENFORCED\n" +
		") WITH (\n" +
		"  'connector' = 'mysql-cdc',\n" +
		"  'hostname' = '10.129.16.82',\n" +
		"  'port' = '3306',\n" +
		"  'username' = 'dp_sync',\n" +
		"  'password' = 'DP#Sync_cifi2020',\n" +
		"  'database-name' = 'saleprj',\n" +
		"  'server-time-zone' = 'Asia/Shanghai',\n" +
		"  'table-name' = 'nos_payable_detail'\n" +
		");"
	return res
}

func BuildDinkySink(fields string) string {
	res := "CREATE TABLE `cdc_test_sink` (\n" +
		fields + "\n" +
		") WITH (\n" +
		"  'connector' = 'doris',\n" +
		"  'fenodes' = '10.129.3.188:8030',\n" +
		"  'table.identifier' = 'odsrt.nos_nos_payable_detail',\n" +
		"  'username' = 'dp_sync',\n" +
		"  'password' = 'DP#Sync_cifi2020',\n" +
		"  'sink.label-prefix' = 'nos_nos_payable_detail',\n" +
		"  'sink.properties.format' = 'json',\n" +
		"  'sink.properties.read_json_by_line' = 'true'\n" +
		");"
	return res
}

func BuildDinkyJob(columns []*ColumnInfo) string {
	res := "INSERT INTO `doris_test_sink`(\n"
	fields := ""
	for _, column := range columns {
		fields += "  `" + column.ColumnName + "`,\n"
	}
	res += fields + "  `eventtime`)\n" +
		"SELECT\n" + fields + "  NOW()\n" + "FROM cdc_test_source;"
	return res
}

func BuildDinkyFieldWithType(columns []*ColumnInfo) string {
	res := ""
	for i, column := range columns {
		if i < len(columns)-1 {
			res += "  `" + column.ColumnName + "` " + TransField2Dinky(column.DataType) + ",\n"
		} else {
			res += "  `" + column.ColumnName + "` " + TransField2Dinky(column.DataType)
		}
	}
	return res
}

// MySQL字段类型到Doris字段类型的映射
var mysqlToDorisType = map[string]string{
	"INT":       "INT",
	"BIGINT":    "BIGINT",
	"VARCHAR":   "VARCHAR",
	"TEXT":      "TEXT",
	"DATE":      "DATE",
	"DATETIME":  "DATETIME",
	"TIMESTAMP": "TIMESTAMP",
	"DECIMAL":   "DECIMAL",
	"DOUBLE":    "DOUBLE",
	"FLOAT":     "FLOAT",
	"TINYINT":   "TINYINT",
	"SMALLINT":  "SMALLINT",
	"CHAR":      "CHAR",
	"BINARY":    "BINARY",
	"BYTES":     "INT",
	"ENUM":      "VARCHAR",
	"LONGTEXT":  "TEXT",
	"int":       "int",
	"bigint":    "bigint",
	"varchar":   "VARCHAR",
	"text":      "TEXT",
	"date":      "DATE",
	"datetime":  "DATETIME",
	"timestamp": "TIMESTAMP",
	"decimal":   "DECIMAL",
	"double":    "DOUBLE",
	"float":     "FLOAT",
	"tinyint":   "TINYINT",
	"smallint":  "SMALLINT",
	"char":      "CHAR",
	"binary":    "BINARY",
	"bytes":     "INT",
	"enum":      "VARCHAR",
	"longtext":  "TEXT",
}

// TransField2Doris
// dataType string  varchar
// columnType string  varchar(100)   text   double(10,2) int(4)
// return varchar(300)     text    double(10,2)  int(4)
func TransField2Doris(dataType string, columnType string) string {
	res := "STRING-注意人工校验!" //默认转为字符串
	if _, ok := mysqlToDorisType[dataType]; !ok {
		return res
	}
	res = mysqlToDorisType[dataType]
	if !strings.Contains(columnType, "(") {
		return res
	}
	num := GetDataTypeNums(columnType)
	if num == "" {
		return res
	}
	if dataType == "varchar" || dataType == "VARCHAR" ||
		dataType == "char" || dataType == "CHAR" {
		len, err := strconv.Atoi(num)
		if err != nil {
			return res
		}
		res += "(" + strconv.Itoa(len*3) + ")"
	} else {
		res += "(" + num + ")"
	}
	return res
}

func GetDataTypeNums(columnType string) string {
	// varchar(100)   double(4,2)  提取出括号中的串
	re := regexp.MustCompile(`[0-9]*\d+`)
	match := re.FindString(columnType)
	if match == "" {
		return ""
	}
	return match
}

// MySQL字段类型到Dinky字段类型的映射
var mysqlToDinkyType = map[string]string{
	"INT":       "INT",
	"BIGINT":    "BIGINT",
	"VARCHAR":   "STRING",
	"TEXT":      "STRING",
	"DATE":      "DATE",
	"DATETIME":  "TIMESTAMP",
	"TIMESTAMP": "TIMESTAMP",
	"DECIMAL":   "DOUBLE",
	"DOUBLE":    "DOUBLE",
	"FLOAT":     "FLOAT",
	"TINYINT":   "TINYINT",
	"SMALLINT":  "SMALLINT",
	"CHAR":      "STRING",
	"BINARY":    "BINARY",
	"BYTES":     "INT",
	"LONGTEXT":  "STRING",
	"int":       "INT",
	"bigint":    "BIGINT",
	"varchar":   "STRING",
	"text":      "STRING",
	"date":      "DATE",
	"datetime":  "TIMESTAMP",
	"timestamp": "TIMESTAMP",
	"decimal":   "DOUBLE",
	"double":    "DOUBLE",
	"float":     "FLOAT",
	"tinyint":   "TINYINT",
	"smallint":  "SMALLINT",
	"char":      "STRING",
	"binary":    "BINARY",
	"bytes":     "INT",
	"longtext":  "STRING",
}

func TransField2Dinky(columnType string) string {
	if _, ok := mysqlToDinkyType[columnType]; ok {
		return mysqlToDinkyType[columnType]
	}
	return "STRING" //默认转为字符串
}
