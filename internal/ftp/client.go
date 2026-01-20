// internal/ftp/client.go
package ftp

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"go-import-file/internal/config"

	"github.com/jlaffaye/ftp"
)

type Client struct {
	conn   *ftp.ServerConn
	config config.FTPConfig
}

func NewClient(cfg config.FTPConfig) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	conn, err := ftp.Dial(addr, ftp.DialWithTimeout(30*time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to FTP server: %w", err)
	}

	if err := conn.Login(cfg.Username, cfg.Password); err != nil {
		conn.Quit()
		return nil, fmt.Errorf("failed to login to FTP server: %w", err)
	}

	return &Client{
		conn:   conn,
		config: cfg,
	}, nil
}

func (c *Client) DownloadFiles(localFolder string) ([]string, error) {
	if err := os.MkdirAll(localFolder, 0755); err != nil {
		return nil, fmt.Errorf("failed to create local folder: %w", err)
	}

	if c.config.RemoteDir != "" {
		if err := c.conn.ChangeDir(c.config.RemoteDir); err != nil {
			return nil, fmt.Errorf("failed to change directory: %w", err)
		}
	}

	entries, err := c.conn.List(".")
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	var downloadedFiles []string

	for _, entry := range entries {
		if entry.Type != ftp.EntryTypeFile {
			continue
		}

		if c.config.FilePattern != "" {
			matched, _ := filepath.Match(c.config.FilePattern, entry.Name)
			if !matched {
				continue
			}
		}

		localPath := filepath.Join(localFolder, entry.Name)

		if err := c.downloadFile(entry.Name, localPath); err != nil {
			return downloadedFiles, fmt.Errorf("failed to download %s: %w", entry.Name, err)
		}

		downloadedFiles = append(downloadedFiles, localPath)

		// LANGSUNG archive/delete file di FTP setelah download sukses
		if c.config.MoveAfterDownload && c.config.ArchiveDir != "" {
			// Move to archive directory
			if err := c.MoveFileWithTimestamp(entry.Name, c.config.ArchiveDir); err != nil {
				fmt.Printf("Warning: Failed to move %s to archive: %v\n", entry.Name, err)
			}
		} else if c.config.DeleteAfterDownload {
			// Delete file
			if err := c.DeleteFile(entry.Name); err != nil {
				fmt.Printf("Warning: Failed to delete %s: %v\n", entry.Name, err)
			}
		}
	}

	return downloadedFiles, nil
}

func (c *Client) downloadFile(remotePath, localPath string) error {
	resp, err := c.conn.Retr(remotePath)
	if err != nil {
		return err
	}
	defer resp.Close()

	localFile, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer localFile.Close()

	_, err = io.Copy(localFile, resp)
	return err
}

// MoveFile moves a file from source to destination directory on FTP server
func (c *Client) MoveFile(sourceFile, destDir string) error {
	// Ensure destination directory exists
	if err := c.ensureDir(destDir); err != nil {
		return fmt.Errorf("failed to ensure destination directory: %w", err)
	}

	// Get just the filename
	filename := filepath.Base(sourceFile)

	// Build destination path
	destPath := filepath.Join(destDir, filename)

	// Use FTP RNFR (rename from) and RNTO (rename to) commands
	if err := c.conn.Rename(sourceFile, destPath); err != nil {
		return fmt.Errorf("failed to move file from %s to %s: %w", sourceFile, destPath, err)
	}

	return nil
}

// MoveFileWithTimestamp moves file with timestamp appended to filename
func (c *Client) MoveFileWithTimestamp(sourceFile, destDir string) error {
	// Pastikan pakai POSIX path
	sourceFile = path.Clean(sourceFile)
	destDir = path.Clean(destDir)

	// Ensure destination directory exists (TIDAK MERUBAH CWD)
	if err := c.ensureDir(destDir); err != nil {
		return fmt.Errorf("failed to ensure destination directory: %w", err)
	}

	// Ambil nama file
	filename := path.Base(sourceFile)
	ext := path.Ext(filename)
	nameWithoutExt := filename[:len(filename)-len(ext)]

	// Timestamp
	timestamp := time.Now().Format("20060102_150405")
	newFilename := fmt.Sprintf("%s_%s%s", nameWithoutExt, timestamp, ext)

	// DESTINATION PATH (ABSOLUTE, POSIX)
	destPath := path.Join(destDir, newFilename)

	// 🔥 FTP RENAME HARUS ABSOLUTE → ABSOLUTE
	if err := c.conn.Rename(sourceFile, destPath); err != nil {
		return fmt.Errorf(
			"failed to move file from [%s] to [%s]: %w",
			sourceFile, destPath, err,
		)
	}

	return nil
}

// DeleteFile deletes a file from FTP server
func (c *Client) DeleteFile(remotePath string) error {
	if err := c.conn.Delete(remotePath); err != nil {
		return fmt.Errorf("failed to delete file %s: %w", remotePath, err)
	}
	return nil
}

// ArchiveProcessedFiles moves or deletes files after successful processing
func (c *Client) ArchiveProcessedFiles(files []string, archiveDir string, deleteAfterArchive bool) error {
	// Change to source directory
	if c.config.RemoteDir != "" {
		if err := c.conn.ChangeDir(c.config.RemoteDir); err != nil {
			return fmt.Errorf("failed to change to source directory: %w", err)
		}
	}

	for _, file := range files {
		filename := filepath.Base(file)

		if archiveDir != "" {
			// Move to archive directory
			if err := c.MoveFileWithTimestamp(filename, archiveDir); err != nil {
				return fmt.Errorf("failed to archive %s: %w", filename, err)
			}
		} else if deleteAfterArchive {
			// Delete file
			if err := c.DeleteFile(filename); err != nil {
				return fmt.Errorf("failed to delete %s: %w", filename, err)
			}
		}
	}

	return nil
}

// ensureDir creates directory if it doesn't exist
func (c *Client) ensureDir(dir string) error {
	dir = path.Clean(dir)

	origDir, err := c.conn.CurrentDir()
	if err != nil {
		return err
	}

	// Cek dir tanpa merusak CWD
	if err := c.conn.ChangeDir(dir); err != nil {
		if err := c.conn.MakeDir(dir); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// BALIK KE DIR AWAL
	_ = c.conn.ChangeDir(origDir)
	return nil
}

// ListFiles lists all files in the current FTP directory
func (c *Client) ListFiles(dir string) ([]string, error) {
	if dir != "" {
		if err := c.conn.ChangeDir(dir); err != nil {
			return nil, fmt.Errorf("failed to change directory: %w", err)
		}
	}

	entries, err := c.conn.List(".")
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if entry.Type == ftp.EntryTypeFile {
			files = append(files, entry.Name)
		}
	}

	return files, nil
}

// CheckFileExists checks if a file exists on FTP server
func (c *Client) CheckFileExists(remotePath string) (bool, error) {
	entries, err := c.conn.List(remotePath)
	if err != nil {
		return false, nil
	}
	return len(entries) > 0, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Quit()
	}
	return nil
}
