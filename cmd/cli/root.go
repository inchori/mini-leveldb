package cli

import (
	"fmt"
	"mini-leveldb/db"
	"os"

	"github.com/spf13/cobra"
)

var (
	dataDir string
	dbh     *db.DB
)

var rootCmd = &cobra.Command{
	Use:   "minildb",
	Short: "Mini LevelDB CLI",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if dbh != nil {
			return nil
		}
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
		newDB, err := db.NewDB(dataDir)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		dbh = newDB
		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if dbh != nil {
			_ = dbh.Close()
		}
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&dataDir, "data-dir", "d", "./data", "Directory to store database files")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func getDB() *db.DB {
	if dbh == nil {
		panic("database not initialized")
	}
	return dbh
}
