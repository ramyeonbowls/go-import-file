# go-import-file

Lightweight Go-based file importer for PDAMASTER-style files (inventory, backorder, prices, invoices).

## Ringkasan

`go-import-file` adalah aplikasi Go yang membaca file transfer (mis. PDAMASTER_*), mem-parse blok data, dan mengimpornya ke database SQL Server. Project ini dipisah menjadi komponen untuk parsing, orkestrasi, logging, dan pengumpulan metrik.

## Fitur utama

- Parsing banyak format blok (lihat `worker/` untuk handler per-block).
- Penulisan bulk ke database via `internal/db`.
- Logging dan metrics per-file/per-job (`logger/`, `metrics/`).
- Folder `transfer/` untuk menampung file sukses/gagal.

## Persyaratan

- Go 1.20+ (sesuaikan dengan `go.mod`).
- Akses ke SQL Server (kredensial dikonfigurasi di `internal/config`).

## Build

Build dari direktori root proyek atau dari `cmd`:

```bash
go build ./cmd
```

Atau jalankan langsung tanpa build:

```bash
go run ./cmd
```

Catatan: contoh sebelumnya menjalankan `go build` di folder `cmd` dengan sukses.

## Konfigurasi

- Konfigurasi utama berada di `internal/config/config.go`.
- Masukkan koneksi database, detail FTP (jika dipakai), dan opsi logging di file konfigurasi atau environment variables sesuai implementasi di `internal/config`.
- Untuk koneksi database lihat `internal/db/sqlserver.go`.

## Struktur proyek (ringkasan)

- `cmd/` – entrypoint aplikasi.
- `internal/config/` – konfigurasi aplikasi.
- `internal/db/` – koneksi & helper SQL Server.
- `internal/ftp/` – client FTP (jika diperlukan).
- `importer/` – implementasi importer per jenis file (mis. `fprice.go`).
- `logger/` – logger per-hari dan worker-specific loggers.
- `metrics/` – pengumpulan dan ekspos metrik proses.
- `model/` – model data dan mapping ke DB.
- `orchestrator/` – orkestrasi proses import untuk tiap tipe data.
- `worker/` – parser dan handler blok untuk setiap record type.
- `utils/` – helper utilities (FS, counter, dll).
- `logs/` – output logging (rotated/daily).
- `transfer/` – tempat file yang di-proses; subfolder `success/` dan `failed/`.

## Folder `transfer/` dan contoh file

Folder `transfer/success/` berisi file yang berhasil diproses. Contoh dari repo menunjukkan beberapa file seperti:

- `PDAMASTER_20260105_172738_IMSTKBAL.txt`
- `PDAMASTER_20260105_172914_MBACKORDER.txt`

File yang gagal diproses dipindah ke `transfer/failed/`.

## Menjalankan (Contoh)

1. Pastikan variabel konfigurasi/database sudah di-set.
2. Build atau run:

```powershell
cd cmd
go build
./cmd.exe
```

atau (tanpa binary):

```bash
go run ./cmd
```

## Logging & Metrics

- Logger harian dan worker-specific tersimpan di `logs/`.
- Metrics dikumpulkan di `metrics/` dan diekspos oleh collector yang ada di paket yang sama.

## Debug & development

- Gunakan `go run ./cmd` selama development.
- Untuk men-debug satu handler blok, buka file terkait di `worker/` (mis. `block01_handler.go`).

## Kontribusi

- Silakan buka issue atau PR untuk penambahan fitur/bugfix.
- Ikuti gaya kode yang sudah ada; jaga perubahan kecil dan fokus.

## License

Tambahkan lisensi proyek Anda di sini (mis. MIT). Jika belum ingin menentukan, tulis `Proprietary` atau `TBD`.

## Kontak

- Pemelihara: tambahkan informasi kontak atau referensi repository internal.

---

File ini dibuat otomatis untuk membantu dokumentasi dasar proyek. Jika Anda ingin saya menambahkan contoh konfigurasi `config.go`, contoh koneksi DB, atau instruksi deployment CI/CD, beri tahu saya.
