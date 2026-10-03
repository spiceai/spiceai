# One snapshot writer per dataset (writer lease)

When several spiced instances create snapshots of the same dataset in the same snapshot location, only one of them uploads. Replicas of a highly available deployment are the usual case. The instances elect that writer with a lease object in the snapshot location, synchronized by the store's conditional writes. There is nothing to configure beyond giving each replica its own identity, which Kubernetes pods already have.

## How it works

1. **Lease object.** Each dataset has its own lease, `<location>/leases/<dataset>.json`.
2. **Taking the lease.** Before creating a snapshot, an instance reads the lease. If there is none, it creates one with `If-None-Match: *`. When several instances race, exactly one create succeeds.
3. **Renewing.** The holder renews the lease each time it is about to create a snapshot, with `If-Match` on the version it read. This includes forced snapshots: the first snapshot of a dataset, and the one taken before an acceleration is recreated.
4. **Standby.** Every other instance skips its snapshots while the lease is live. The acceleration is still checkpointed locally. The instance logs once that another instance holds the lease:

   ```text
   Dataset 'orders' is not creating snapshots while instance 'spice-1' holds its snapshot writer lease; this instance takes over if that lease goes 20m without renewal.
   ```

5. **Lease duration.** A lease lasts twice the dataset's snapshot interval, and between 30 seconds and 24 hours:

   | Snapshot trigger | Interval |
   |---|---|
   | `snapshots_trigger: time_interval` | `snapshots_trigger_threshold` (default `10m`) |
   | `refresh_complete` (the default for full and append refreshes) | `refresh_check_interval`, or `10m` without one |
   | `stream_batches` | `10m` |

   The holder writes its duration into the lease, and the other instances go by that duration.
6. **Takeover.** An instance takes over a lease that it has seen unchanged for the lease's duration, measured on its own clock. The store's clock and the holder's clock don't enter into it, so clock skew can't expire a live lease. The takeover is written with `If-Match`, so again exactly one instance wins.
   - The new holder logs the takeover at info.
   - The previous holder, if it is still running, logs a warning and stops creating snapshots.
7. **Failed snapshots release the lease.** A holder whose snapshot fails, for example because its local disk is full, releases the lease. Another instance can then take it over at its next snapshot.
8. **Out-of-order snapshots are not published.** Every change of holder increments the lease's generation. The snapshot metadata records the generation that published each dataset's latest snapshot, as the dataset property `writer-generation`. A holder that loses the lease in the middle of an upload therefore can't publish over the newer snapshot of the instance that took over. It logs a warning, and its upload stays in the location unreferenced.
9. **Instance identity.** An instance is identified by `SPICE_INSTANCE_ID`, or its host name without one. This is the same identity [Postgres replication](postgres-replication.md#multi-replica-deployments) uses for replication slots.
   - The identity is stable across restarts, so a restarted replica resumes the lease it held.
   - Replicas that share one identity both create snapshots. When one finds the other's renewal under its identity, it logs a warning: give each replica a distinct `SPICE_INSTANCE_ID`.
10. **Stores without conditional writes.** If the snapshot location rejects conditional writes, the instance creates snapshots without a lease, as it would without this feature, and logs a warning once. Amazon S3, Google Cloud Storage, and Azure Blob Storage support conditional writes.
11. **S3 event notifications.** Lease writes are `ObjectCreated` events under the snapshot location. The [snapshot notification](acceleration-snapshot-notifications.md) consumer deletes them without reloading anything.

The lease object:

```json
{
  "format-version": 1,
  "holder-identity": "spice-1",
  "holder-process": "0199e2a4-6c1b-7d1e-9a4f-3b2c1d0e9f8a",
  "generation": 3,
  "lease-duration-ms": 1200000,
  "acquire-time-ms": 1790000000000,
  "renew-time-ms": 1790000600000
}
```

## Considerations

- **Failover time.** After the holder stops, the other instances see its lease unchanged. They take it over at their first snapshot after one lease duration, which is about twice the snapshot interval.
- **Refreshes on a standby.** A refresh that runs only on a standby, for example one triggered through `POST /v1/datasets/{name}/acceleration/refresh`, updates that instance's acceleration only. Its snapshot is not uploaded while another instance holds the lease.
- **A holder that stops getting data.** With `snapshots_trigger: time_interval`, the holder renews the lease on every interval, even when its refreshes are failing. Other replicas therefore don't take over from a holder whose source is broken but whose process runs. With `refresh_complete`, a failed refresh creates no snapshot and renews nothing, so the lease passes on. Restart or remove a replica whose refreshes keep failing.
- **Slow snapshots.** If a snapshot takes longer than the lease duration, the lease can pass to another instance mid-upload. The writer generation keeps the older upload from being published over the newer one, but both instances do the work. Keep the snapshot interval above half the time a snapshot takes.
