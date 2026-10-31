import { useRef, useState } from "react";
import { getLikes, likeOnce, unlikeOnce } from "./api";

// Each button fires this many individual POST /likes requests, in concurrent batches,
// to simulate a burst of real users liking the same video at once.
const LIKE_BURST_OPTIONS = [100, 500, 1000, 2000, 5000];
// Phase 3 moved PostgreSQL off the request path entirely (Redis INCR + async Kafka event),
// so the hot path is now bottlenecked by the browser/network, not the backend - batches can
// be larger still than Phase 2's Redis-synchronous-Postgres design.
const BATCH_SIZE = 200;

interface VideoRow {
  videoId: number;
  likeCount: number | null;
  progress: { done: number; total: number } | null;
  lastBurst: { amount: number; durationMs: number; requestsPerSecond: number } | null;
  error: string | null;
}

export default function App() {
  const [rows, setRows] = useState<VideoRow[]>([
    { videoId: 1001, likeCount: null, progress: null, lastBurst: null, error: null },
    { videoId: 1002, likeCount: null, progress: null, lastBurst: null, error: null },
    { videoId: 1003, likeCount: null, progress: null, lastBurst: null, error: null },
  ]);
  const [newVideoId, setNewVideoId] = useState(1004);
  const busyVideoIds = useRef(new Set<number>());

  const updateRow = (videoId: number, patch: Partial<VideoRow>) => {
    setRows((prev) => prev.map((row) => (row.videoId === videoId ? { ...row, ...patch } : row)));
  };

  const addVideo = () => {
    setRows((prev) => {
      if (prev.some((row) => row.videoId === newVideoId)) return prev;
      return [...prev, { videoId: newVideoId, likeCount: null, progress: null, lastBurst: null, error: null }];
    });
    setNewVideoId((id) => id + 1);
  };

  const removeVideo = (videoId: number) => {
    setRows((prev) => prev.filter((row) => row.videoId !== videoId));
  };

  const handleRefresh = async (videoId: number) => {
    updateRow(videoId, { error: null });
    try {
      const result = await getLikes(videoId);
      updateRow(videoId, { likeCount: result.likeCount });
    } catch (err) {
      updateRow(videoId, { error: err instanceof Error ? err.message : "Failed to load like count" });
    }
  };

  const handleUnlike = async (videoId: number) => {
    updateRow(videoId, { error: null });
    try {
      const result = await unlikeOnce(videoId);
      updateRow(videoId, { likeCount: result.likeCount });
    } catch (err) {
      updateRow(videoId, { error: err instanceof Error ? err.message : "Failed to unlike video" });
    }
  };

  const simulateBurst = async (videoId: number, amount: number) => {
    if (busyVideoIds.current.has(videoId)) return;
    busyVideoIds.current.add(videoId);
    updateRow(videoId, { error: null, lastBurst: null, progress: { done: 0, total: amount } });
    const startedAt = performance.now();

    try {
      let done = 0;
      while (done < amount) {
        const batchSize = Math.min(BATCH_SIZE, amount - done);
        await Promise.all(Array.from({ length: batchSize }, () => likeOnce(videoId)));
        done += batchSize;
        updateRow(videoId, { progress: { done, total: amount } });
      }
      const durationMs = performance.now() - startedAt;
      const result = await getLikes(videoId);
      updateRow(videoId, {
        likeCount: result.likeCount,
        lastBurst: { amount, durationMs, requestsPerSecond: amount / (durationMs / 1000) },
      });
    } catch (err) {
      updateRow(videoId, { error: err instanceof Error ? err.message : "Failed to send like burst" });
    } finally {
      busyVideoIds.current.delete(videoId);
      updateRow(videoId, { progress: null });
    }
  };

  return (
    <main className="container">
      <h1>Video Like-Counter</h1>
      <p className="consistency-note">
        Counts shown here come straight from Redis (instant). PostgreSQL is updated asynchronously
        via Kafka, usually within a couple of seconds - eventual, not strong, consistency.
      </p>

      <section className="panel">
        <label htmlFor="newVideoId">Add video ID</label>
        <input
          id="newVideoId"
          type="number"
          min={1}
          value={newVideoId}
          onChange={(e) => setNewVideoId(Number(e.target.value))}
        />
        <button onClick={addVideo}>Add video</button>
      </section>

      <table className="video-table">
        <thead>
          <tr>
            <th>Video ID</th>
            <th>Likes</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const busy = row.progress !== null;
            return (
              <tr key={row.videoId}>
                <td>{row.videoId}</td>
                <td className="like-count-cell">
                  {row.likeCount ?? "—"}
                  {row.progress && (
                    <div className="progress">
                      {row.progress.done} / {row.progress.total}
                    </div>
                  )}
                  {row.lastBurst && (
                    <div className="throughput">
                      {row.lastBurst.amount.toLocaleString()} likes in {(row.lastBurst.durationMs / 1000).toFixed(2)}s
                      ({row.lastBurst.requestsPerSecond.toFixed(0)} req/s)
                    </div>
                  )}
                  {row.error && <div className="error">{row.error}</div>}
                </td>
                <td className="actions-cell">
                  <button onClick={() => handleRefresh(row.videoId)} disabled={busy}>
                    Refresh
                  </button>
                  <button onClick={() => handleUnlike(row.videoId)} disabled={busy}>
                    Unlike (-1)
                  </button>
                  {LIKE_BURST_OPTIONS.map((amount) => (
                    <button key={amount} onClick={() => simulateBurst(row.videoId, amount)} disabled={busy}>
                      +{amount.toLocaleString()}
                    </button>
                  ))}
                  <button className="remove-button" onClick={() => removeVideo(row.videoId)} disabled={busy}>
                    Remove
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </main>
  );
}
