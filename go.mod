module github.com/FlashbackAi/teepin-go

// Go 1.26 for the standard library's crypto/hpke (owner, 2026-11-06; TEEPIN-STORAGE-DESIGN.md section 25.3).
go 1.26.0

// v0.2.1 sent storage bytes through the API's old routes, which the service no longer accepts; use the latest version.
retract v0.2.1

// v0.3.0 and its release candidate send packs under 10 MiB, which the service refuses from 2026-11-12; use the latest.
retract [v0.3.0-rc.1, v0.3.0]
