package main

import (
	"context"
	"flag"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"

	"go-import-file/internal/config"
	"go-import-file/internal/db"
	"go-import-file/internal/ftp"
	"go-import-file/internal/orchestrator"
	"go-import-file/internal/utils"
)

func main() {
	block := flag.String("block", "", "Block filter ex: MPRICE")
	flag.Parse()

	blockID := strings.TrimSpace(*block)
	if blockID == "" {
		log.Fatalf("No block specified")
	}

	start := time.Now()
	ctx := context.Background()

	cfg := config.Load()

	for _, dir := range []string{
		cfg.FilePath,
		cfg.FileDir,
		cfg.FileSuccessDir,
		cfg.FileFailedDir,
		cfg.LogsDir,
	} {
		if err := utils.EnsureDir(dir); err != nil {
			log.Fatalf("Failed to create dir %s: %v", dir, err)
		}
	}

	dbConn, err := db.NewSQLServer(
		cfg.DBHost,
		cfg.DBPort,
		cfg.DBUser,
		cfg.DBPass,
		cfg.DBName,
	)
	if err != nil {
		log.Fatalf("DB connection failed: %v", err)
	}
	defer dbConn.Close()

	// Initialize FTP client
	ftpClient, err := ftp.NewClient(cfg.FTP)
	if err != nil {
		log.Fatalf("Failed to create FTP client: %v", err)
	}
	defer ftpClient.Close()

	// Download files from FTP
	// File akan LANGSUNG di-move/delete setelah download
	log.Println("Starting FTP download...")
	files, err := ftpClient.DownloadFiles(cfg.FilePath)
	if err != nil {
		log.Fatalf("Failed to download files: %v", err)
	}
	log.Printf("Downloaded %d files (files moved/deleted from FTP immediately)", len(files))

	processID := uuid.New().String()
	chain := orchestrator.New()

	log.Printf("Process ID %s\n", processID)

	// =========================================================
	// BLOCK REGISTRY
	// =========================================================
	blocks := map[string][]struct {
		Name string
		Fn   func(context.Context) error
	}{
		"MPRICE": {
			{
				Name: "IMPORT MPRICE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMPrice(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
			{
				Name: "FINALIZE MPRICE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMPriceFinalizeIdempotent(
						ctx,
						dbConn,
						processID,
					)
				},
			},
		},
		"MPRICEGRP": {
			{
				Name: "IMPORT MPRICEGRP",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMPriceGrp(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MCUST": {
			{
				Name: "IMPORT MCUST",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMCust(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MSKU": {
			{
				Name: "IMPORT MSKU",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMsku(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MCUSTGRP": {
			{
				Name: "IMPORT MCUSTGRP",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMCustGrp(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MCUSTINDUS": {
			{
				Name: "IMPORT MCUSTINDUS",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMCustIndus(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MSALESMAN": {
			{
				Name: "IMPORT MSALESMAN",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMsalesman(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"SLSINV": {
			{
				Name: "IMPORT SLSINV",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunSlsInv(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"ARINVOICE": {
			{
				Name: "IMPORT ARINVOICE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunArInvoice(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"IMSTKBAL": {
			{
				Name: "IMPORT IMSTKBAL",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunImStkbal(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MBACKORDER": {
			{
				Name: "IMPORT MBACKORDER",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMBackOrder(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MBEAT": {
			{
				Name: "IMPORT MBEAT",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMBeat(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MCUSTCL": {
			{
				Name: "IMPORT MCUSTCL",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMCustCl(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MCUSTINVD": {
			{
				Name: "IMPORT MCUSTINVD",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMCustInvD(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MCUSTINVH": {
			{
				Name: "IMPORT MCUSTINVH",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMCustInvH(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MCUSTTYPE": {
			{
				Name: "IMPORT MCUSTTYPE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMCustType(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MDISTRICT": {
			{
				Name: "IMPORT MDISTRICT",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMDistrict(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MKAT": {
			{
				Name: "IMPORT MKAT",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMKat(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MKPLPRICE": {
			{
				Name: "IMPORT MKPLPRICE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMkplPrice(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
			{
				Name: "FINALIZE MKPLPRICE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMkplPriceFinalizeIdempotent(
						ctx,
						dbConn,
						processID,
					)
				},
			},
		},
		"MMARKET": {
			{
				Name: "IMPORT MMARKET",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMmarket(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MPAYERTO": {
			{
				Name: "IMPORT MPAYERTO",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMPayerTo(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MPROVINCE": {
			{
				Name: "IMPORT MPROVINCE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMProvince(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MRUTE": {
			{
				Name: "IMPORT MRUTE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMRute(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MSBRAND": {
			{
				Name: "IMPORT MSBRAND",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMSBrand(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MSHIPTO": {
			{
				Name: "IMPORT MSHIPTO",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMShipTo(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MSLINE": {
			{
				Name: "IMPORT MSLINE",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMSline(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MSUBBEAT": {
			{
				Name: "IMPORT MSUBBEAT",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMSubBeat(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MSUBBRAND": {
			{
				Name: "IMPORT MSUBBRAND",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMSubBrand(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
		"MTOP": {
			{
				Name: "IMPORT MTOP",
				Fn: func(ctx context.Context) error {
					return orchestrator.RunMTop(
						ctx,
						dbConn,
						cfg.FilePath,
						processID,
					)
				},
			},
		},
	}

	// =========================================================
	// EXECUTION
	// =========================================================
	if blockID == "" {
		for blockName, steps := range blocks {
			log.Printf("Register block: %s\n", blockName)
			for _, step := range steps {
				chain.Add(step.Name, step.Fn)
			}
		}
	} else {
		steps, ok := blocks[blockID]
		if !ok {
			log.Fatalf("Unknown block: %s", blockID)
		}

		log.Printf("Running block: %s\n", blockID)
		for _, step := range steps {
			chain.Add(step.Name, step.Fn)
		}
	}

	if err := chain.Run(ctx); err != nil {
		log.Fatalf("IMPORT FAILED: %v", err)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Alloc=%dMB Sys=%dMB", m.Alloc/1024/1024, m.Sys/1024/1024)
	log.Printf("ALL IMPORTS COMPLETED IN %s\n", time.Since(start))
}
