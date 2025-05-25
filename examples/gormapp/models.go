/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
