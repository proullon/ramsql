package main

import (
	"fmt"

	"github.com/astaxie/beego/orm"
	_ "github.com/proullon/ramsql/driver"
)

// Model Struct
type User struct {
	Id   int
	Name string `orm:"size(100)"`
}

func init() {
	// register model
	orm.RegisterModel(new(User))

	// set default database
	orm.RegisterDataBase("default", "ramsql", "root:root@/my_db?charset=utf8", 30)
}

func main() {
	o := orm.NewOrm()

	user := User{Name: "slene"}

	// insert
	id, err := o.Insert(&user)
	fmt.Printf("ID: %d, ERR: %v\n", id, err)

	// update
	user.Name = "astaxie"
	num, err := o.Update(&user)
	fmt.Printf("NUM: %d, ERR: %v\n", num, err)

	// read one
	u := User{Id: user.Id}
	err = o.Read(&u)
	fmt.Printf("ERR: %v\n", err)

	// delete
	num, err = o.Delete(&u)
	fmt.Printf("NUM: %d, ERR: %v\n", num, err)
}
