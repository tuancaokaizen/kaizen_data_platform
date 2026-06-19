import axios from "axios";

const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "",
  timeout: 20000
});

export type EmbedDashboardInfo = {
  slug: string;
  embeddedUuid: string;
};

export type EmbedConfig = {
  supersetDomain: string;
  enabled: boolean;
  dashboards: EmbedDashboardInfo[];
};

export type GuestTokenResponse = {
  token: string;
  embeddedUuid: string;
  slug: string;
};

export async function fetchEmbedConfig(): Promise<EmbedConfig> {
  const response = await apiClient.get<EmbedConfig>("/api/v1/bi/embed-config");
  return response.data;
}

export async function fetchGuestToken(slug: string): Promise<GuestTokenResponse> {
  const response = await apiClient.post<GuestTokenResponse>("/api/v1/bi/guest-token", {
    slug
  });
  return response.data;
}
