package ramsql

import (
	"database/sql"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code  string
	Price uint
}

// From https://gorm.io/docs/connecting_to_the_database.html
// and  https://gorm.io/docs/
func TestGormQuickStart(t *testing.T) {
	ramdb, err := sql.Open("ramsql", "TestGormQuickStart")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: ramdb,
	}),
		&gorm.Config{})
	if err != nil {
		t.Fatalf("cannot setup gorm: %s", err)
	}

	// Migrate the schema
	err = db.AutoMigrate(&Product{})
	if err != nil {
		t.Fatalf("cannot automigrate: %s", err)
	}

	// Create
	db.Create(&Product{Code: "D42", Price: 100})
	if err != nil {
		t.Fatalf("cannot create: %s", err)
	}

	// Read
	var product Product
	db.First(&product, 1) // find product with integer primary key
	if err != nil {
		t.Fatalf("cannot read with primary key: %s", err)
	}
	db.First(&product, "code = ?", "D42") // find product with code D42
	if err != nil {
		t.Fatalf("cannot read with code: %s", err)
	}

	// Update - update product's price to 200
	db.Model(&product).Update("Price", 200)
	if err != nil {
		t.Fatalf("cannot update: %s", err)
	}
	// Update - update multiple fields
	db.Model(&product).Updates(Product{Price: 200, Code: "F42"}) // non-zero fields
	if err != nil {
		t.Fatalf("cannot update multiple fields: %s", err)
	}
	db.Model(&product).Updates(map[string]interface{}{"Price": 200, "Code": "F42"})
	if err != nil {
		t.Fatalf("cannot update multiple fields: %s", err)
	}

	// Delete - delete product
	db.Delete(&product, 1)
	if err != nil {
		t.Fatalf("cannot delete: %s", err)
	}
}
