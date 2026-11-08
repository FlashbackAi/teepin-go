module github.com/FlashbackAi/teepin-go

// Go 1.26 for the standard library's crypto/hpke (owner, 2026-11-06; TEEPIN-STORAGE-DESIGN.md section 25.3).
go 1.26.0

// v0.2.1 sent storage bytes through the API's old routes, which the service no longer accepts; use the latest version.
retract v0.2.1
