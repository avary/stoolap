package main

import (
	"time"
)

// User represents a user in the system
type User struct {
	ID        int64  `gorm:"primaryKey"`
	Name      string `gorm:"size:255;not null"`
	Email     string `gorm:"size:255;uniqueIndex"`
	Age       int
	Active    bool // Removed default value
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Product represents a product in the inventory
type Product struct {
	ID          int64  `gorm:"primaryKey"`
	Name        string `gorm:"size:255;not null"`
	Description string `gorm:"type:text"`
	Price       float64
	Stock       int // Removed default value
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
