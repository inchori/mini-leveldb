package cli

import "github.com/spf13/cobra"

var flushCmd = &cobra.Command{
	Use:   "flush",
	Short: "Flush the MemTable to SSTable files",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := getDB().Flush(); err != nil {
			return err
		}
		cmd.Println("Flushed MemTable to SSTable files")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(flushCmd)
}
