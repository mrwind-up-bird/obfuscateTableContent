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
	"unicode"

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

func getConnectionStringSourceDb() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		viper.GetString("databaseSource.username"),
		viper.GetString("databaseSource.password"),
		viper.GetString("databaseSource.host"),
		viper.GetString("databaseSource.port"),
		viper.GetString("databaseSource.dbname"),
	)
}

func getConnectionStringTargetDb() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		viper.GetString("databaseTarget.username"),
		viper.GetString("databaseTarget.password"),
		viper.GetString("databaseTarget.host"),
		viper.GetString("databaseTarget.port"),
		viper.GetString("databaseTarget.dbname"),
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
		if _, ok := row[field]; ok {
			obfuscatedValue := obfuscateString(row[field])
			row[field] = obfuscatedValue
			//fmt.Printf("Obfuscate %s field: %v to new Value: %s\n", field, value, obfuscatedValue)
		}
	}
}

func obfuscateString(input string) string {
	if len(input) <= 3 {
		return "***"
	}

	if len(input) <= 6 {
		masked := input[:1]
		for i := 1; i < len(input)-1; i++ {
			if isSpecialCharacter(input[i]) {
				masked += string(input[i])
			} else {
				masked += "*"
			}
		}
		masked += input[len(input)-1:]

		return masked
	}

	masked := input[:3]
	for i := 3; i < len(input)-3; i++ {
		if isSpecialCharacter(input[i]) {
			masked += string(input[i])
		} else {
			masked += "*"
		}
	}

	masked += input[len(input)-3:]

	return masked
}

func isSpecialCharacter(char byte) bool {
	return !unicode.IsLetter(rune(char)) && !unicode.IsNumber(rune(char))
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

	db, err := sql.Open("mysql", getConnectionStringSourceDb())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	for tableName, fieldsToObfuscate := range getSettings() {
		fmt.Printf("\nTable: %s\n", tableName)
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

			obfuscateRow(row, fieldsToObfuscate)

			replaceStatement := generateReplaceStatement(tableName, row, fieldsToObfuscate)
			fmt.Println(replaceStatement)

		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}
}
