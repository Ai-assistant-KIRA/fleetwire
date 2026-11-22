# The device's local log

Every reading is appended to a segmented write-ahead log in
`$SNAP_DATA/wal/` before it can be sent (ADR-0004).

## Format

```
00000000000000000001.seg   name = first seq in the segment (20 digits)
00000000000000041872.seg
state.json                 acked seq, max_seq_ever, pending gaps, gap history
```

Each segment starts with a 16-byte header (`FWWAL\0`, format version 1,
reserved bytes), followed by records:

```
length u32 | crc32c u32 | seq u64 | payload [length]   (little-endian)
```

The CRC (Castagnoli) covers the seq and the payload. The payload is an
encoded `fleetwire.v1.Reading`. Records are at most 64 KiB, so a corrupted
length field cannot cause a huge allocation. Seqs inside a segment are
consecutive.

## Sync modes and what each can lose

| `wal.sync` | A reading becomes durable | Power cut can lose | Process crash can lose |
|---|---|---|---|
| `always` | after the fsync that follows each append | nothing | nothing |
| `interval` (default, `wal.sync-interval` 1 s) | at the next periodic fsync | readings from the last interval that **never left the device** | nothing |
| `never` | when the write reaches the page cache | sent and unsent readings: the guarantee does not hold for power loss | nothing |

Only durable readings are sent. In `always` and `interval` modes, a reading
lost in a power cut was never sent, so its seq can be reused safely.

## Recovery on start

1. Segments are scanned in name order; header, length, CRC and seq order
   are checked.
2. A bad record in the **last** segment is a torn write: the file is cut
   there and fsynced.
3. Damage in a **middle** segment is real corruption: the rest of that
   file is kept as `*.corrupt`, and the missing seqs become a `CORRUPTION`
   gap.
4. Any seq up to the highest one the device can prove it handed out that is
   neither on disk, acked, nor in a gap becomes a `LOST` gap.
5. A sparse index (one entry per 64 records) makes reads after a rewind
   cheap.

`state.json` is replaced atomically (temp file, fsync, rename, fsync of the
directory). A corrupt state file stops the agent with a clear error rather
than guessing, because starting from zero could reuse seqs;
[Recover a device's local log](../how-to/recover-device-state.md) has the
repair.

## Disk budget

When the log grows past `wal.max-size` (default 256 MiB), the oldest
segment is dropped. Its unacked part becomes a `DISK_BUDGET` gap, which is
persisted **before** the file is deleted and reported to the server until
an ack covers it. `fleetwire_agent_readings_dropped_total` counts the loss.
