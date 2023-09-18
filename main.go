package main

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"strings"
)

var (
	// 定义常量
	sourceHost     = "10.122.37.20"
	sourcePort     = "3306"
	sourceUser     = "root"
	sourcePassword = "dolphin"
	sourceDatabase = "dlink"
	sourceTable    = "dlink_alert_group"
)

func main() {

	//dsn := "root:dolphin@tcp(10.122.37.20:3306)/flinkcdc?charset=utf8mb4&parseTime=True&loc=Local"

	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True&loc=Local",
		sourceUser, sourcePassword, sourceHost, sourcePort, sourceDatabase)
	//fmt.Println(dsn)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("Failed to connect database: " + err.Error())
	}
	sqlDB, err := db.DB()
	if err != nil {
		panic(err.Error())
	}
	defer sqlDB.Close()

	GetMysqlTableDDL(db, sourceTable)
	cols := GetAllColumnInfo(db, sourceDatabase, sourceTable)
	_, key := BuildDorisDDLStatement(cols, sourceTable)
	BuildFlinkSQL(cols, key)
}

func ParseParams() {
	// 获取命令行参数
	args := os.Args

	// 遍历命令行参数
	for _, arg := range args {
		// 按照自定义的键值对分隔符进行分割
		parts := strings.Split(arg, "=")
		if len(parts) == 2 {
			key := parts[0]
			value := parts[1]
			fmt.Printf("键: %s, 值: %s\n", key, value)
		}
	}
}

func canFinish(numCourses int, prerequisites [][]int) bool {
	m1 := make(map[int]int, 0) //两课程之间的关系
	m2 := make(map[int]int, 0) //所有课程
	for _, v := range prerequisites {
		m1[v[0]] = v[1]
		m2[v[0]] = 1
		m2[v[1]] = 1
	}
	if numCourses < len(m2) {
		return false
	}
	m3 := make(map[int]int, 0) //已经学过的课程
	for k, _ := range m2 {
		nums := 0
		t := k
		for true {
			if nums > 1 && t == k {
				return false
			}
			if _, ok := m3[t]; ok { //已经学过的直接跳过
				break
			}
			if _, ok := m1[t]; !ok { //没有需要先学别的课程的课直接加入已经学过
				m3[t] = 1
				m3[k] = 1
				break
			} else { //有其他科需要学的课程进行迭代
				t = m1[t]
				nums++
				continue
			}
		}
		m3[k] = 1
	}
	if numCourses >= len(m3) && len(m3) == len(m2) {
		return true
	}

	return true
}
