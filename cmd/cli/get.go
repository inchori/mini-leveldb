package cli

import "github.com/spf13/cobra"

var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Get the value for a key from the database",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		value, err := getDB().Get(key)
		if err != nil {
			return err
		}
		if value == "" {
			return cmd.Help()
		}
		cmd.Println(value)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
}
