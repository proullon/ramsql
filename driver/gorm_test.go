package ramsql

import (
	"database/sql"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code       string
	Price      uint
	TestBigint uint64 `gorm:"test_bigint;type:BIGINT UNSIGNED AUTO_INCREMENT"`
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
	err = db.Create(&Product{Code: "D42", Price: 100}).Error
	if err != nil {
		t.Fatalf("cannot create: %s", err)
	}

	var id uint
	err = ramdb.QueryRow(`SELECT id FROM products WHERE id = 1 AND deleted_at IS NULL`).Scan(&id)
	if err != nil {
		t.Fatalf("cannot select manually: %s", err)
	}
	if id == 0 {
		t.Fatalf("unexpected 0 value for id")
	}

	// Read
	var product Product
	err = db.First(&product, 1).Error // find product with integer primary key
	if err != nil {
		t.Fatalf("cannot read with primary key: %s", err)
	}
	err = db.First(&product, "code = ?", "D42").Error // find product with code D42
	if err != nil {
		t.Fatalf("cannot read with code: %s", err)
	}
	err = db.First(&product, "Code = ?", "D42").Error // find product with code D42
	if err != nil {
		t.Fatalf("cannot read with Code: %s", err)
	}

	// Update - update product's price to 200
	err = db.Model(&product).Update("Price", 200).Error
	if err != nil {
		t.Fatalf("cannot update: %s", err)
	}
	// Update - update multiple fields
	err = db.Model(&product).Updates(Product{Price: 200, Code: "F42"}).Error // non-zero fields
	if err != nil {
		t.Fatalf("cannot update multiple fields 1: %s", err)
	}
	err = db.Model(&product).Updates(map[string]interface{}{"Price": 200, "Code": "F42"}).Error
	if err != nil {
		t.Fatalf("cannot update multiple fields 2: %s", err)
	}

	// Delete - delete product
	err = db.Delete(&product, 1).Error
	if err != nil {
		t.Fatalf("cannot delete: %s", err)
	}
}
