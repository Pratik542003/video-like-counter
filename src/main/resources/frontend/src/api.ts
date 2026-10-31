import type { LikeCountResponse, ApiErrorBody } from "./types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

async function callLikesEndpoint(method: "GET" | "POST" | "DELETE", videoId: number): Promise<LikeCountResponse> {
  const response = await fetch(`${API_BASE_URL}/api/videos/${videoId}/likes`, { method });

  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as ApiErrorBody | null;
    throw new Error(body?.message ?? `Request failed with status ${response.status}`);
  }

  return response.json() as Promise<LikeCountResponse>;
}

export const getLikes = (videoId: number) => callLikesEndpoint("GET", videoId);
export const likeOnce = (videoId: number) => callLikesEndpoint("POST", videoId);
export const unlikeOnce = (videoId: number) => callLikesEndpoint("DELETE", videoId);
