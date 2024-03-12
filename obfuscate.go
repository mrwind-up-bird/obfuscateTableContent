/*
@author: JTL-Software oliver.baer@jtl-software.com
*/
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

func readConfig() {
	viper.SetConfigType("toml")
	configData, err := os.ReadFile("config.txt")
	if err != nil {
		log.Fatal(err)
	}

	err = viper.ReadConfig(strings.NewReader(string(configData)))
	if err != nil {
		log.Fatal(err)
	}
}

func getConnectionString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		viper.GetString("database.username"),
		viper.GetString("database.password"),
		viper.GetString("database.host"),
		viper.GetString("database.port"),
		viper.GetString("database.dbname"),
	)
}

func getSettings() map[string][]string {
	settings := make(map[string][]string)
	for tableName, fields := range viper.GetStringMapString("tables") {
		fieldsSlice := strings.Split(fields, ",")
		settings[tableName] = fieldsSlice
	}
	return settings
}

func obfuscateRow(row map[string]string, fieldsToObfuscate []string) {
	for _, field := range fieldsToObfuscate {
		if value, ok := row[field]; ok {
			obfuscatedValue := obfuscateString(row[field])
			row[field] = obfuscatedValue
			fmt.Printf("Obfuscate %s field: %v to new Value: %s\n", field, value, obfuscatedValue)
		}
	}
}

func obfuscateString(input string) string {
	if len(input) > 4 {
		return input[:3] + "xxx" + input[len(input)-3:]
	}
	return input
}

func generateReplaceStatement(tableName string, row map[string]string, fieldsToObfuscate []string) string {
	columns := strings.Join(fieldsToObfuscate, ",")
	values := make([]string, len(fieldsToObfuscate))

	for i, field := range fieldsToObfuscate {
		values[i] = fmt.Sprintf("'%s'", row[field])
	}

	return fmt.Sprintf("REPLACE INTO %s (%s) VALUES (%s);", tableName, columns, strings.Join(values, ","))
}

func main() {

	readConfig()

	db, err := sql.Open("mysql", getConnectionString())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	for tableName, fieldsToObfuscate := range getSettings() {
		fmt.Printf("\nTable: %s", tableName)
		fieldsString := strings.Join(fieldsToObfuscate, ",")
		query := fmt.Sprintf("SELECT %s FROM %s LIMIT 50", fieldsString, tableName)
		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}

		values := make([]interface{}, len(columns))
		for i := range columns {
			values[i] = new(sql.RawBytes)
		}

		for rows.Next() {
			row := make(map[string]string)
			err := rows.Scan(values...)
			if err != nil {
				log.Fatal(err)
			}

			for i, column := range columns {
				row[column] = string(*values[i].(*sql.RawBytes))
			}
			fmt.Printf("------------\n")
			obfuscateRow(row, fieldsToObfuscate)

			fmt.Println(row)
			fmt.Print("\n")

			replaceStatement := generateReplaceStatement(tableName, row, fieldsToObfuscate)
			fmt.Println(replaceStatement)
			fmt.Print("\n")
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}
}
