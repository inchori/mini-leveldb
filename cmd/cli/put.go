package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var putCmd = &cobra.Command{
	Use:   "put [key] [value]",
	Short: "Put a key-value pair into the database",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return cmd.Help()
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		value := strings.Join(args[1:], " ")
		if err := getDB().Put(key, value); err != nil {
			return fmt.Errorf("failed to put key %s: %w", key, err)
		}
		fmt.Println("OK")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(putCmd)
}
